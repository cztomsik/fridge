const std = @import("std");
const Connection = @import("connection.zig").Connection;
const Session = @import("session.zig").Session;
const Statement = @import("statement.zig").Statement;
const Value = @import("value.zig").Value;
const SqlBuf = @import("sql.zig").SqlBuf;

pub fn createDb(ddl: []const u8) !Session {
    var db = try Session.open(@import("sqlite.zig").SQLite3, std.testing.allocator, .{ .filename = ":memory:" });
    errdefer db.deinit();

    try db.conn.execAll(ddl);

    return db;
}

pub fn fakeDb() !Session {
    return Session.open(TestConn, std.testing.allocator, {});
}

pub fn expectSql(q: anytype, expected: []const u8) !void {
    var buf = try SqlBuf.init(std.testing.allocator);
    defer buf.deinit();

    try buf.append(q);
    try std.testing.expectEqualStrings(expected, buf.buf.items);
}

pub fn expectDdl(db: *Session, object_name: []const u8, expected: []const u8) !void {
    try std.testing.expectEqualStrings(
        expected,
        (try db.raw("SELECT sql FROM sqlite_master", {}).where("name = ?", object_name).get([]const u8)).?,
    );
}

pub const TestConn = struct {
    id: u32,

    pub const Options = void;

    pub var created: std.atomic.Value(u32) = .{ .raw = 0 };
    pub var destroyed: std.atomic.Value(u32) = .{ .raw = 0 };

    pub fn open(_: std.mem.Allocator, _: void) !*TestConn {
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

    pub fn prepare(_: *TestConn, _: []const u8, _: []const Value) !Statement {
        unreachable;
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
