const std = @import("std");
const util = @import("util.zig");

// TODO: So far, it's just a polymorphic string builder, but given that it's
//       the lowest level and it's directly used by the raw query builder, we
//       could also do some dialect translation here.
pub const SqlBuf = struct {
    buf: std.array_list.Managed(u8),

    pub fn init(allocator: std.mem.Allocator) !SqlBuf {
        return .{ .buf = try .initCapacity(allocator, 1024) };
    }

    pub fn deinit(self: *SqlBuf) void {
        self.buf.deinit();
    }

    pub fn appendIdent(self: *SqlBuf, name: []const u8) !void {
        try self.buf.append('"');

        for (name) |char| {
            if (char == '"') {
                try self.buf.appendSlice("\"\"");
            } else {
                try self.buf.append(char);
            }
        }

        try self.buf.append('"');
    }

    pub fn append(self: *SqlBuf, x: anytype) std.mem.Allocator.Error!void {
        if (comptime std.meta.hasMethod(@TypeOf(x), "toSql")) {
            return x.toSql(self);
        }

        if (comptime util.isString(@TypeOf(x))) {
            return self.buf.appendSlice(x);
        }

        @compileError("unsupported type " ++ @typeName(@TypeOf(x)));
    }
};
