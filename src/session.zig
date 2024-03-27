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

    /// Create a new record.
    pub fn create(self: *Session, comptime T: type, data: anytype) !T {
        try self.exec(dsl.insert(T).values(data));
        return try self.find(T, @intCast(try self.conn.lastInsertRowId())) orelse @panic("concurrent write");
    }

    /// Update a record by its primary key
    pub fn update(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id), data: anytype) !void {
        try self.exec(dsl.update(T).set(data).where(.{ .id = id }));
    }

    /// Delete a record by its primary key.
    pub fn delete(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id)) !void {
        return self.exec(dsl.delete(T).where(.{ .id = id }));
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
        res.ptr = @ptrCast(&rows[0]);
        res.len = rows.len;

        return res;
    }

    fn readRow(self: *Session, comptime T: type, stmt: *sqlite.Statement) !T {
        var res: T = undefined;

        inline for (std.meta.fields(@TypeOf(res)), 0..) |f, i| {
            if (comptime @typeInfo(f.type) == .Struct) {
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

const Binder = struct {
    arena: std.mem.Allocator,
    stmt: sqlite.Statement,
    i: usize = 0,

    pub fn bind(self: *Binder, value: anytype) !void {
        defer self.i += 1;

        if (comptime @typeInfo(@TypeOf(value)) == .Struct) {
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
