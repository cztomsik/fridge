const std = @import("std");

pub fn build(b: *std.Build) void {
    _ = b.addModule("ava-sqlite", .{
        .root_source_file = .{ .path = "src/sqlite.zig" },
    });
}
