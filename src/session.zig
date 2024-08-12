const std = @import("std");
const sqlite = @import("sqlite.zig");
const util = @import("util.zig");
const Connection = @import("connection.zig").Connection;
const Statement = @import("statement.zig").Statement;
const Query = @import("query.zig").Query;

pub const Session = struct {
    arena: std.mem.Allocator,
    conn: Connection,

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

    /// Find a record by its primary key.
    pub fn find(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id)) !?T {
        return self.query(T).where(.id, id).findFirst();
    }

    /// Update a record by its primary key.
    pub fn update(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id), data: T) !void {
        return self.patch(T, id, data);
    }

    /// Patch a record by its primary key.
    pub fn patch(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id), data: anytype) !void {
        comptime util.checkFields(T, @TypeOf(data));

        return self.query(T).where(.id, id).update(data).exec();
    }

    /// Delete a record by its primary key.
    pub fn delete(self: *Session, comptime T: type, id: std.meta.FieldType(T, .id)) !void {
        try self.query(T).where(.id, id).delete().exec();
    }
};
