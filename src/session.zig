const std = @import("std");
const sqlite = @import("sqlite.zig");
const util = @import("util.zig");
const Connection = @import("connection.zig").Connection;
const Pool = @import("pool.zig").Pool;
const Statement = @import("statement.zig").Statement;
const Query = @import("query.zig").Query;

pub const Session = struct {
    arena: std.mem.Allocator,
    conn: Connection,
    pool: ?*Pool = null,
    close: bool = false,

    pub fn open(comptime T: type, allocator: std.mem.Allocator, options: T.Options) !Session {
        var sess = try Session.init(allocator, try Connection.open(T, options));
        sess.close = true;
        return sess;
    }

    pub fn init(allocator: std.mem.Allocator, conn: Connection) !Session {
        const arena = try allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(allocator);

        return .{
            .arena = arena.allocator(),
            .conn = conn,
        };
    }

    pub fn deinit(self: *Session) void {
        const arena: *std.heap.ArenaAllocator = @ptrCast(@alignCast(self.arena.ptr));
        arena.deinit();
        arena.child_allocator.destroy(arena);

        if (self.pool) |pool| {
            pool.releaseConnection(self.conn);
        } else {
            if (self.close) {
                self.conn.close();
            }
        }
    }

    pub fn prepare(self: *Session, sql: []const u8, args: anytype) !Statement {
        var stmt: Statement = try self.conn.prepare(sql);
        errdefer stmt.deinit();

        stmt.session = self;
        try stmt.bindAll(args);
        return stmt;
    }

    // TODO: begin/commit/rollback via self.conn.execAll(...)?

    pub fn query(self: *Session, comptime T: type) Query(T, T) {
        return .{ .session = self };
    }

    // TODO: this is useless without filter, ordering, paging, ...
    //       and I'm not sure if we should order by primary key anyway
    // /// Find all records of the given type.
    // pub fn findAll(self: *Session, comptime T: type) ![]const T {
    //     return self.query(T).findAll();
    // }

    /// Find a record by its primary key.
    pub fn find(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id)) !?T {
        return self.query(T).firstWhere(.id, id);
    }

    /// Insert a new record.
    pub fn insert(self: *Session, comptime T: type, data: anytype) !void {
        comptime util.checkFields(T, @TypeOf(data));

        return self.query(T).insert(data).exec();
    }

    /// Update a record by its primary key.
    pub fn update(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id), data: anytype) !void {
        comptime util.checkFields(T, @TypeOf(data));

        return self.query(T).where(.id, id).update(data).exec();
    }

    /// Delete a record by its primary key.
    pub fn delete(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id)) !void {
        try self.query(T).where(.id, id).delete().exec();
    }
};

const t = std.testing;

const Person = struct {
    id: u32,
    name: []const u8,
};

fn open() !Session {
    var conn = try Connection.open(@import("sqlite.zig").SQLite3, .{ .filename = ":memory:" });
    errdefer conn.close();

    try conn.execAll(
        \\CREATE TABLE Person (id INTEGER PRIMARY KEY, name TEXT);
        \\INSERT INTO Person (name) VALUES ('Alice');
        \\INSERT INTO Person (name) VALUES ('Bob');
    );

    return Session.init(t.allocator, conn);
}

fn close(db: *Session) void {
    db.deinit();
    db.conn.close();
}

test "db.prepare()" {
    var db = try open();
    defer close(&db);

    var stmt = try db.prepare("SELECT 1 + ?", .{1});
    defer stmt.deinit();

    try t.expectEqual(2, try stmt.value(u32));
}

test "db.query(T).findAll()" {
    var db = try open();
    defer close(&db);

    try t.expectEqualDeep(&[_]Person{
        .{ .id = 1, .name = "Alice" },
        .{ .id = 2, .name = "Bob" },
    }, db.query(Person).all());
}

test "find(T, id)" {
    var db = try open();
    defer close(&db);

    try t.expectEqualDeep(
        Person{ .id = 1, .name = "Alice" },
        db.find(Person, 1),
    );
}

test "db.insert(T, data)" {
    var db = try open();
    defer close(&db);

    try db.insert(Person, .{ .name = "Charlie" });
    try t.expectEqualDeep(3, db.conn.lastInsertRowId());
    try t.expectEqual(1, db.conn.rowsAffected());
}

test "db.update(T, id, data)" {
    var db = try open();
    defer close(&db);

    try db.update(Person, 1, .{ .name = "Sarah" });
    try t.expectEqual(1, db.conn.rowsAffected());
    try t.expectEqualDeep(Person{ .id = 1, .name = "Sarah" }, db.find(Person, 1));
}

test "db.delete(T, id)" {
    var db = try open();
    defer close(&db);

    try db.delete(Person, 1);
    try t.expectEqual(1, db.conn.rowsAffected());
    try t.expectEqual(null, db.find(Person, 1));
}
