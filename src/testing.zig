const std = @import("std");
const Session = @import("session.zig").Session;

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
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    try q.toSql(&buf);
    try std.testing.expectEqualStrings(expected, buf.items);
}
