const std = @import("std");
const util = @import("util.zig");

pub const Value = union(enum) {
    null,
    bool: bool,
    int: i64,
    float: f64,
    string: []const u8,
    blob: []const u8,

    pub fn from(val: anytype, arena: std.mem.Allocator) !Value {
        return switch (@TypeOf(val)) {
            Value => val,
            else => |T| {
                if (comptime std.meta.hasFn(T, "toValue")) {
                    return T.toValue(val, arena);
                }

                if (comptime util.isString(T)) {
                    return .{ .string = val };
                }

                return switch (@typeInfo(T)) {
                    .Optional => if (val) |v| from(v, arena) else .null,
                    .Bool => .{ .bool = val },
                    .Int, .ComptimeInt => .{ .int = @intCast(val) },
                    .Float, .ComptimeFloat => .{ .float = @floatCast(val) },
                    else => @compileError("TODO: " ++ @typeName(T)),
                };
            },
        };
    }
};

pub const Blob = struct {
    bytes: []const u8,

    pub fn toValue(self: Blob, _: std.mem.Allocator) Value {
        return .{ .blob = self.bytes };
    }
};
