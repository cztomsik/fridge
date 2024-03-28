const std = @import("std");
const dsl = @import("dsl.zig");
const sqlite = @import("sqlite.zig");
const Pool = @import("pool.zig").Pool;

pub const Session = struct {
    arena: std.mem.Allocator,
    conn: sqlite.SQLite3,
    pool: ?*Pool = null,
    buf: std.ArrayList(u8),

    /// Create a new session from a connection.
    pub fn fromConnection(arena: std.mem.Allocator, conn: sqlite.SQLite3) Session {
        return .{
            .arena = arena,
            .conn = conn,
            .buf = std.ArrayList(u8).init(arena),
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
    pub fn prepare(self: *Session, queryable: anytype) !sqlite.Statement {
        if (comptime dsl.isString(@TypeOf(queryable))) {
            return self.conn.prepare(queryable);
        }

        defer self.buf.clearRetainingCapacity();
        try queryable.sql(&self.buf);

        var binder = Binder{
            .arena = self.arena,
            .stmt = try self.conn.prepare(self.buf.items),
        };

        try queryable.bind(&binder);
        return binder.stmt;
    }

    /// Execute a query.
    pub fn exec(self: *Session, queryable: anytype) !void {
        var stmt = try self.prepare(queryable);
        defer stmt.deinit();

        try stmt.exec();
    }

    /// Insert a new record.
    pub fn insert(self: *Session, comptime T: type, data: anytype) !void {
        try self.exec(dsl.insert(T).values(data));
    }

    /// Update a record by its primary key
    pub fn update(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id), data: anytype) !void {
        try self.exec(dsl.update(T).set(data).where(.{ .id = id }));
    }

    /// Delete a record by its primary key.
    pub fn delete(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id)) !void {
        return self.exec(dsl.delete(T).where(.{ .id = id }));
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

    /// Return the first record for the given query.
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
        var stmt = try self.prepare(query.count());
        defer stmt.deinit();

        _ = try stmt.step();
        return try stmt.column(u64, 0);
    }

    fn readRow(self: *Session, comptime T: type, stmt: *sqlite.Statement) !T {
        var res: T = undefined;

        inline for (std.meta.fields(@TypeOf(res)), 0..) |f, i| {
            if (comptime @typeInfo(f.type) == .Struct and f.type != sqlite.Blob) {
                @field(res, f.name) = try std.json.parseFromSliceLeaky(
                    f.type,
                    self.arena,
                    try stmt.column([]const u8, i),
                    .{},
                );
                continue;
            }

            @field(res, f.name) = try self.dupe(
                try stmt.column(f.type, i),
            );
        }

        return res;
    }

    fn dupe(self: *Session, value: anytype) std.mem.Allocator.Error!@TypeOf(value) {
        return switch (@TypeOf(value)) {
            sqlite.Blob => sqlite.Blob{ .bytes = try self.arena.dupe(u8, value.bytes) },
            []const u8 => self.arena.dupe(u8, value),
            [:0]const u8 => self.arena.dupeZ(u8, value),
            else => |T| switch (@typeInfo(T)) {
                .Optional => try self.dupe(value orelse return null),
                .Struct => @compileError("TODO"),
                else => value,
            },
        };
    }
};

const Binder = struct {
    arena: std.mem.Allocator,
    stmt: sqlite.Statement,
    i: usize = 0,

    pub fn bind(self: *Binder, value: anytype) !void {
        defer self.i += 1;

        if (comptime @typeInfo(@TypeOf(value)) == .Struct and @TypeOf(value) != sqlite.Blob) {
            return self.stmt.bind(
                self.i,
                try std.json.stringifyAlloc(
                    self.arena,
                    value,
                    .{},
                ),
            );
        }

        try self.stmt.bind(self.i, value);
    }
};

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
    try std.testing.expectEqualDeep(1, db.conn.rowsAffected());
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

test "pluck(query, field)" {
    var db = try sess();
    defer cleanup(&db);

    const names = try db.pluck(dsl.query(Person), .name);

    try std.testing.expectEqualDeep(&[_][]const u8{
        "Alice",
        "Bob",
    }, names);
}
