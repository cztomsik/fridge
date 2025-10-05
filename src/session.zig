const std = @import("std");
const sqlite = @import("sqlite.zig");
const util = @import("util.zig");
const Connection = @import("connection.zig").Connection;
const Pool = @import("pool.zig").Pool;
const Statement = @import("statement.zig").Statement;
const RawQuery = @import("raw.zig").Query;
const Query = @import("query.zig").Query;
const Value = @import("value.zig").Value;
const Schema = @import("schema.zig").Schema;

pub const Session = struct {
    arena: std.mem.Allocator,
    conn: Connection,

    /// Generic shorthand for `Session.init(T.open(allocator, options))`
    pub fn open(comptime T: type, allocator: std.mem.Allocator, options: T.Options) !Session {
        const conn = try Connection.open(T, allocator, options);
        errdefer conn.deinit();

        return .init(allocator, conn);
    }

    /// Create a new session (taking ownership of the connection)
    pub fn init(allocator: std.mem.Allocator, conn: Connection) !Session {
        const arena = try allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(allocator);

        return .{
            .arena = arena.allocator(),
            .conn = conn,
        };
    }

    /// Close the session (including the connection)
    pub fn deinit(self: *Session) void {
        const arena: *std.heap.ArenaAllocator = @ptrCast(@alignCast(self.arena.ptr));
        arena.deinit();
        arena.child_allocator.destroy(arena);

        self.conn.deinit();
    }

    pub fn prepare(self: *Session, sql: []const u8, args: anytype) !Statement {
        const values = try self.arena.alloc(Value, args.len);
        inline for (0..args.len) |i| {
            values[i] = try Value.from(args[i], self.arena);
        }

        return self.conn.prepare(sql, values);
    }

    // TODO: begin/commit/rollback via self.conn.execAll(...)?

    pub fn exec(self: *Session, sql: []const u8, args: anytype) !void {
        var stmt = try self.prepare(sql, args);
        defer stmt.deinit();

        try stmt.exec();
    }

    pub fn raw(self: *Session, sql: []const u8, args: anytype) RawQuery {
        return RawQuery.raw(self, sql, args);
    }

    pub fn query(self: *Session, comptime T: type) Query(T) {
        return .init(self);
    }

    pub fn schema(self: *Session) Schema {
        return .init(self);
    }

    // TODO: this is useless without filter, ordering, paging, ...
    //       and I'm not sure if we should order by primary key anyway
    // /// Find all records of the given type.
    // pub fn findAll(self: *Session, comptime T: type) ![]const T {
    //     return self.query(T).findAll();
    // }

    /// Find a record by its primary key.
    pub fn find(self: *Session, comptime T: type, id: util.Id(T)) !?T {
        return self.query(T).find(id);
    }

    /// Shorthand for insert() + find()
    pub fn create(self: *Session, comptime T: type, data: T) !T {
        const id = try self.insert(T, data);
        return try self.find(T, id) orelse unreachable;
    }

    /// Insert a new record and return its primary key
    pub fn insert(self: *Session, comptime T: type, data: anytype) !util.Id(T) {
        try self.query(T).insert(data).exec(); // TODO: returning id?
        return @intCast(try self.conn.lastInsertRowId());
    }

    /// Update a record by its primary key.
    pub fn update(self: *Session, comptime T: type, id: util.Id(T), data: anytype) !void {
        return self.query(T).where("id", id).update(data).exec();
    }

    /// Delete a record by its primary key.
    pub fn delete(self: *Session, comptime T: type, id: util.Id(T)) !void {
        try self.query(T).where("id", id).delete().exec();
    }
};

const t = std.testing;
const createDb = @import("testing.zig").createDb;

const Person = struct {
    id: u32,
    name: []const u8,
};

const ddl =
    \\CREATE TABLE Person (id INTEGER PRIMARY KEY, name TEXT);
    \\INSERT INTO Person (name) VALUES ('Alice');
    \\INSERT INTO Person (name) VALUES ('Bob');
;

test "db.prepare()" {
    var db = try createDb(ddl);
    defer db.deinit();

    var stmt = try db.prepare("SELECT 1 + ?", .{1});
    defer stmt.deinit();

    try t.expectEqual(.{2}, stmt.next(struct { u32 }, db.arena));
}

test "db.exec()" {
    var db = try createDb(ddl);
    defer db.deinit();

    try db.exec("INSERT INTO Person (name) VALUES (?)", .{"Charlie"});
    try t.expectEqual(3, db.conn.lastInsertRowId());
    try t.expectEqual(1, db.conn.rowsAffected());
}

test "db.raw()" {
    var db = try createDb(ddl);
    defer db.deinit();

    try t.expectEqual(1, try db.raw("SELECT 1", {}).get(u32));
    try t.expectEqual(3, try db.raw("SELECT 1 + ?", 2).get(u32));
    try t.expectEqualSlices(u32, &.{ 1, 2 }, try db.raw("SELECT id FROM Person", {}).pluck(u32));
}

test "db.query(T).xxx() value methods" {
    var db = try createDb(ddl);
    defer db.deinit();

    var q = db.query(Person);

    try t.expectEqual(true, q.exists());
    try t.expectEqual(false, q.where("id", 3).exists());
    try t.expectEqual(2, q.count("id"));
    try t.expectEqual(1, q.min("id"));
    try t.expectEqual(2, q.max("id"));
    try t.expectEqual(null, q.where("id", 3).max("id"));
    try t.expectEqualSlices(u32, &.{ 1, 2 }, try q.pluck("id"));
}

test "db.query(T).findAll()" {
    var db = try createDb(ddl);
    defer db.deinit();

    try t.expectEqualDeep(&[_]Person{
        .{ .id = 1, .name = "Alice" },
        .{ .id = 2, .name = "Bob" },
    }, db.query(Person).findAll());
}

test "db.find(T, id)" {
    var db = try createDb(ddl);
    defer db.deinit();

    try t.expectEqualDeep(
        Person{ .id = 1, .name = "Alice" },
        db.find(Person, 1),
    );
}

test "db.insert(T, data)" {
    var db = try createDb(ddl);
    defer db.deinit();

    _ = try db.insert(Person, .{ .name = "Charlie" });
    try t.expectEqual(3, db.conn.lastInsertRowId());
    try t.expectEqual(1, db.conn.rowsAffected());
}

test "db.update(T, id, data)" {
    var db = try createDb(ddl);
    defer db.deinit();

    try db.update(Person, 1, .{ .name = "Sarah" });
    try t.expectEqual(1, db.conn.rowsAffected());
    try t.expectEqualDeep(Person{ .id = 1, .name = "Sarah" }, db.find(Person, 1));
}

test "db.delete(T, id)" {
    var db = try createDb(ddl);
    defer db.deinit();

    try db.delete(Person, 1);
    try t.expectEqual(1, db.conn.rowsAffected());
    try t.expectEqual(null, db.find(Person, 1));
}
