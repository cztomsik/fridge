const std = @import("std");
const dsl = @import("dsl.zig");
const sqlite = @import("sqlite.zig");
const util = @import("util.zig");
const Pool = @import("pool.zig").Pool;

pub const Session = struct {
    arena: std.mem.Allocator,
    conn: sqlite.SQLite3,
    pool: ?*Pool = null,

    /// Create a new session from a connection.
    pub fn fromConnection(arena: std.mem.Allocator, conn: sqlite.SQLite3) Session {
        return .{
            .arena = arena,
            .conn = conn,
        };
    }

    /// Create a new session from a pool.
    pub fn fromPool(arena: std.mem.Allocator, pool: *Pool) Session {
        var session = fromConnection(arena, pool.get());
        session.pool = pool;
        return session;
    }

    /// Deinitialize the session.
    pub fn deinit(self: *Session) void {
        if (self.pool) |pool| {
            pool.release(self.conn);
        } else {
            self.conn.close();
        }
    }

    /// Prepare a query into a statement.
    pub fn prepare(self: *Session, query: anytype) !sqlite.Statement {
        if (comptime util.isString(@TypeOf(query))) {
            return self.conn.prepare(query);
        }

        var buf = try std.ArrayList(u8).initCapacity(self.arena, 512);
        try query.sql(&buf);

        var binder = Binder{
            .arena = self.arena,
            .stmt = try self.conn.prepare(buf.items),
        };

        try query.bind(&binder);
        return binder.stmt;
    }

    /// Execute a query.
    pub fn exec(self: *Session, query: anytype) !void {
        var stmt = try self.prepare(query);
        defer stmt.deinit();

        try stmt.exec();
    }

    /// Insert a new record.
    pub fn insert(self: *Session, comptime T: type, data: anytype) !void {
        try self.exec(dsl.query(T).insert(data));
    }

    /// Update a record by its primary key
    pub fn update(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id), data: anytype) !void {
        try self.exec(dsl.query(T).where(.{ .id = id }).update(data));
    }

    /// Delete a record by its primary key.
    pub fn delete(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id)) !void {
        try self.deleteBy(T, .{ .id = id });
    }

    /// Delete records matching the given criteria.
    pub fn deleteBy(self: *Session, comptime T: type, criteria: anytype) !void {
        try self.exec(dsl.query(T).where(criteria).delete());
    }

    /// Create a new record and return it.
    pub fn create(self: *Session, comptime T: type, data: anytype) !T {
        try self.insert(T, data);
        return try self.find(T, @intCast(try self.conn.lastInsertRowId())) orelse @panic("concurrent write");
    }

    /// Find a record by its primary key.
    pub fn find(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id)) !?T {
        return try self.findBy(T, .{ .id = id });
    }

    /// Find a record matching the given criteria.
    pub fn findBy(self: *Session, comptime T: type, criteria: anytype) !?T {
        return self.findOne(dsl.query(T).where(criteria));
    }

    /// Find a record for the given query.
    pub fn findOne(self: *Session, query: anytype) !?@TypeOf(query).Row {
        var stmt = try self.prepare(query);
        defer stmt.deinit();

        if (try stmt.step() == .done) {
            return null;
        }

        return try self.readRow(@TypeOf(query).Row, &stmt);
    }

    /// Return all records for the given query.
    pub fn findAll(self: *Session, query: anytype) ![]@TypeOf(query).Row {
        var res = std.ArrayList(@TypeOf(query).Row).init(self.arena);
        var stmt = try self.prepare(query);
        defer stmt.deinit();

        while (try stmt.step() == .row) {
            try res.append(
                try self.readRow(@TypeOf(query).Row, &stmt),
            );
        }

        return res.toOwnedSlice();
    }

    /// Find a single value for the given query.
    pub fn findValue(self: *Session, comptime T: type, query: anytype) !?T {
        var stmt = try self.prepare(query);
        defer stmt.deinit();

        if (try stmt.step() == .done) {
            return null;
        }

        return try self.readValue(T, &stmt, 0);
    }

    /// Return all values for a given field.
    pub fn pluck(self: *Session, query: anytype, comptime field: std.meta.FieldEnum(@TypeOf(query).Row)) ![]const std.meta.FieldType(@TypeOf(query).Row, field) {
        const rows = try self.findAll(query.select(&.{field}));
        var res: []std.meta.FieldType(@TypeOf(query).Row, field) = undefined;
        res.ptr = @ptrCast(rows.ptr);
        res.len = rows.len;

        return res;
    }

    /// Return the number of records for the given query.
    pub fn count(self: *Session, query: anytype) !u64 {
        return (try self.findValue(u64, query.count())).?;
    }

    fn readRow(self: *Session, comptime T: type, stmt: *sqlite.Statement) !T {
        var res: T = undefined;

        inline for (std.meta.fields(@TypeOf(res)), 0..) |f, i| {
            @field(res, f.name) = try self.readValue(f.type, stmt, i);
        }

        return res;
    }

    fn readValue(self: *Session, comptime T: type, stmt: *sqlite.Statement, i: usize) !T {
        if (comptime @typeInfo(T) == .Optional) {
            return if (stmt.isNull(i)) null else try self.readValue(@typeInfo(T).Optional.child, stmt, i);
        }

        if (comptime isJsonType(T)) {
            return std.json.parseFromSliceLeaky(
                T,
                self.arena,
                try stmt.column([]const u8, i),
                .{ .allocate = .alloc_always },
            );
        }

        return switch (T) {
            sqlite.Blob => sqlite.Blob{ .bytes = try self.readValue([]const u8, stmt, i) },
            []const u8, [:0]const u8 => self.arena.dupeZ(u8, try stmt.column([]const u8, i)),
            else => try stmt.column(T, i),
        };
    }
};

const Binder = struct {
    arena: std.mem.Allocator,
    stmt: sqlite.Statement,
    i: usize = 0,

    pub fn bind(self: *Binder, value: anytype) !void {
        if (comptime @typeInfo(@TypeOf(value)) == .Optional) {
            return if (value) |v| self.bind(v) else self.bind(null);
        }

        if (comptime isJsonType(@TypeOf(value))) {
            try self.stmt.bind(
                self.i,
                try std.json.stringifyAlloc(
                    self.arena,
                    value,
                    .{},
                ),
            );
        } else {
            try self.stmt.bind(self.i, value);
        }

        self.i += 1;
    }
};

fn isJsonType(comptime T: type) bool {
    return T != sqlite.Blob and switch (@typeInfo(T)) {
        .Array, .Struct => true,
        .Pointer => |p| p.size == .Slice and p.child != u8,
        else => false,
    };
}

const Person = struct {
    id: u32,
    name: []const u8,
};

fn sess() !Session {
    var arena = try std.testing.allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(std.testing.allocator);

    var conn = try sqlite.SQLite3.open(":memory:");
    errdefer conn.close();

    try conn.execAll(
        \\CREATE TABLE Person (id INTEGER PRIMARY KEY, name TEXT);
        \\INSERT INTO Person (name) VALUES ('Alice');
        \\INSERT INTO Person (name) VALUES ('Bob');
    );

    return Session.fromConnection(arena.allocator(), conn);
}

fn cleanup(db: *Session) void {
    const arena: *std.heap.ArenaAllocator = @ptrCast(@alignCast(db.arena.ptr));
    db.deinit();
    arena.deinit();
    std.testing.allocator.destroy(arena);
}

test "exec()" {
    var db = try sess();
    defer cleanup(&db);

    try db.exec("INSERT INTO Person (name) VALUES ('Charlie')");
    try std.testing.expectEqualDeep(3, db.conn.lastInsertRowId());
}

test "insert(T, data)" {
    var db = try sess();
    defer cleanup(&db);

    try db.insert(Person, .{ .name = "Charlie" });
    try std.testing.expectEqualDeep(3, db.conn.lastInsertRowId());
}

test "update(T, id, data)" {
    var db = try sess();
    defer cleanup(&db);

    try db.update(Person, 1, .{ .name = "Sarah" });
    const person = try db.find(Person, 1) orelse return error.NotFound;
    try std.testing.expectEqualDeep(Person{ .id = 1, .name = "Sarah" }, person);
}

test "delete(T, id)" {
    var db = try sess();
    defer cleanup(&db);

    try db.delete(Person, 1);
    try std.testing.expectEqual(1, db.conn.rowsAffected());
    try std.testing.expectEqualDeep(null, db.find(Person, 1));
}

test "deleteBy(T, criteria)" {
    var db = try sess();
    defer cleanup(&db);

    try db.deleteBy(Person, .{ .name = "Alice" });
    try std.testing.expectEqual(1, db.conn.rowsAffected());
    try std.testing.expectEqualDeep(null, db.find(Person, 1));
}

test "find(T, id)" {
    var db = try sess();
    defer cleanup(&db);

    const person = try db.find(Person, 1) orelse return error.NotFound;
    try std.testing.expectEqualDeep(Person{ .id = 1, .name = "Alice" }, person);
}

test "findBy(T, criteria)" {
    var db = try sess();
    defer cleanup(&db);

    const person = try db.findBy(Person, .{ .name = "Alice" }) orelse return error.NotFound;
    try std.testing.expectEqualDeep(Person{ .id = 1, .name = "Alice" }, person);
}

test "findOne(query)" {
    var db = try sess();
    defer cleanup(&db);

    const person = try db.findOne(dsl.query(Person).where(.{ .name = "Alice" })) orelse return error.NotFound;
    try std.testing.expectEqualDeep(Person{ .id = 1, .name = "Alice" }, person);
}

test "findAll(query)" {
    var db = try sess();
    defer cleanup(&db);

    try std.testing.expectEqualDeep(&[_]Person{
        .{ .id = 1, .name = "Alice" },
        .{ .id = 2, .name = "Bob" },
    }, db.findAll(dsl.query(Person)));
}

test "findAll(raw)" {
    var db = try sess();
    defer cleanup(&db);

    const Row = struct {
        id: u32,
        name: []const u8,
    };

    const rows = try db.findAll(dsl.raw("SELECT * FROM Person WHERE id = ?", .{1}).as(Row));

    try std.testing.expectEqualDeep(&[_]Row{
        .{ .id = 1, .name = "Alice" },
    }, rows);
}

test "pluck(query, field)" {
    var db = try sess();
    defer cleanup(&db);

    const names = try db.pluck(dsl.query(Person), .name);

    try std.testing.expectEqualDeep(&[_][]const u8{
        "Alice",
        "Bob",
    }, names);
}
