const std = @import("std");
const Session = @import("session.zig").Session;
const SqlBuf = @import("sql.zig").SqlBuf;

pub fn createDb(ddl: []const u8) !Session {
    var db = try Session.open(@import("sqlite.zig").SQLite3, std.testing.allocator, .{ .filename = ":memory:" });
    errdefer db.deinit();

    try db.conn.execAll(ddl);

    return db;
}

pub fn fakeDb() !Session {
    return Session.init(std.testing.allocator, undefined);
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
