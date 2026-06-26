const std = @import("std");
const Connection = @import("connection.zig").Connection;
const Session = @import("session.zig").Session;
const Statement = @import("statement.zig").Statement;
const Value = @import("value.zig").Value;
const Error = @import("error.zig").Error;
const util = @import("util.zig");

pub fn createDb(ddl: []const u8) !Session {
    var db = try Session.open(@import("sqlite.zig").SQLite3, std.testing.allocator, std.testing.io, .{ .filename = ":memory:" });
    errdefer db.deinit();

    try db.conn.execAll(ddl);

    return db;
}

pub fn fakeDb() !Session {
    return Session.open(TestConn, std.testing.allocator, std.testing.io, {});
}

pub fn expectSql(q: anytype, expected: []const u8) !void {
    _ = try q.prepare();
    try std.testing.expectEqualStrings(expected, TestConn.last_sql);
}

pub fn expectDdl(db: *Session, object_name: []const u8, expected: []const u8) !void {
    try std.testing.expectEqualStrings(
        expected,
        (try db.raw("SELECT sql FROM sqlite_master WHERE name = ?", .{object_name}).get([]const u8)).?,
    );
}

pub const TestConn = struct {
    id: u32,

    pub const Options = void;

    pub var created: std.atomic.Value(u32) = .{ .raw = 0 };
    pub var destroyed: std.atomic.Value(u32) = .{ .raw = 0 };
    pub var last_sql: []const u8 = "";

    pub fn open(_: std.mem.Allocator, _: std.Io, _: void) !*TestConn {
        const ptr = try std.testing.allocator.create(TestConn);
        ptr.id = created.fetchAdd(1, .monotonic);
        return ptr;
    }

    pub fn dialect(_: *TestConn) Connection.Dialect {
        unreachable;
    }

    pub fn execAll(_: *TestConn, _: []const u8) !void {
        unreachable;
    }

    pub fn prepare(_: *TestConn, sql: []const u8) !Statement {
        last_sql = sql;
        return util.upcast(@as(*TestStmt, undefined), Statement);
    }

    pub fn rowsAffected(_: *TestConn) !usize {
        unreachable;
    }

    pub fn lastInsertRowId(_: *TestConn) !i64 {
        unreachable;
    }

    pub fn lastError(_: *TestConn) []const u8 {
        unreachable;
    }

    pub fn deinit(self: *TestConn) void {
        std.testing.allocator.destroy(self);
        _ = destroyed.fetchAdd(1, .monotonic);
    }
};

const TestStmt = struct {
    pub fn bind(_: *TestStmt, _: usize, _: Value) Error!void {}

    pub fn columnCount(_: *TestStmt) usize {
        return 0;
    }

    pub fn columnName(_: *TestStmt, _: usize) []const u8 {
        return "";
    }

    pub fn column(_: *TestStmt, _: usize) Error!Value {
        return .null;
    }

    pub fn step(_: *TestStmt) Error!bool {
        return false;
    }

    pub fn reset(_: *TestStmt) Error!void {}

    pub fn deinit(_: *TestStmt) void {}
};
