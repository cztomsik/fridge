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
        if (comptime isString(@TypeOf(queryable))) {
            return self.conn.prepare(queryable);
        }

        defer self.buf.clearRetainingCapacity();
        try queryable.sql(&self.buf);

        var i: usize = 0;
        var stmt = try self.conn.prepare(self.buf.items);
        try queryable.bind(&stmt, &i);
        return stmt;
    }

    /// Execute a query.
    pub fn exec(self: *Session, queryable: anytype) !void {
        var stmt = try self.prepare(queryable);
        defer stmt.deinit();

        try stmt.exec();
    }

    /// Create a new record.
    pub fn create(self: *Session, comptime T: type, data: anytype) !T {
        try self.exec(dsl.insert(T).values(data));
        return self.find(T, @intCast(try self.conn.lastInsertRowId()));
    }

    /// Update a record by its primary key
    pub fn update(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id), data: anytype) !T {
        try self.exec(dsl.update(T).set(data).where(.{ .id = id }));
        return self.find(T, id);
    }

    /// Delete a record by its primary key.
    pub fn delete(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id)) !void {
        return self.exec(dsl.delete(T).where(.{ .id = id }));
    }

    /// Find a record by its primary key.
    pub fn find(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id)) !T {
        return try self.findBy(T, .{ .id = id }) orelse error.NotFound;
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

    fn readRow(self: *Session, comptime T: type, stmt: *sqlite.Statement) !T {
        var res: T = undefined;

        // TODO: json deserialization
        inline for (std.meta.fields(@TypeOf(res)), 0..) |f, i| {
            @field(res, f.name) = try self.dupe(
                try stmt.column(f.type, i),
            );
        }

        return res;
    }

    fn dupe(self: *Session, value: anytype) std.mem.Allocator.Error!@TypeOf(value) {
        return switch (@TypeOf(value)) {
            sqlite.Blob => sqlite.Blob{try self.arena.dupe(u8, value)},
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

fn isString(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .Pointer => |ptr| ptr.child == u8 or switch (@typeInfo(ptr.child)) {
            .Array => |arr| arr.child == u8,
            else => false,
        },
        else => false,
    };
}
