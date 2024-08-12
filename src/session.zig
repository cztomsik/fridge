const std = @import("std");
const sqlite = @import("sqlite.zig");
const util = @import("util.zig");
const Connection = @import("connection.zig").Connection;
const Statement = @import("statement.zig").Statement;
const Query = @import("query.zig").Query;
const Repo = @import("repo.zig").Repo;

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

    pub fn repo(self: *Session, comptime T: type) Repo(T) {
        return .{ .session = self };
    }
};
