const std = @import("std");
const util = @import("util.zig");

pub const Value = union(enum) {
    null,
    int: i64,
    float: f64,
    string: []const u8,
    blob: []const u8,

    pub fn from(val: anytype, arena: std.mem.Allocator) !Value {
        const T = @TypeOf(val);

        if (comptime T == Value) {
            return val;
        }

        if (comptime std.meta.hasFn(T, "toValue")) {
            return T.toValue(val, arena);
        }

        if (comptime util.isString(T)) {
            return .{ .string = val };
        }

        return switch (@typeInfo(T)) {
            .Null => .null,
            .Optional => if (val) |v| from(v, arena) else .null,
            .Bool => .{ .int = if (val) 1 else 0 },
            .Int, .ComptimeInt => .{ .int = @intCast(val) },
            .Float, .ComptimeFloat => .{ .float = @floatCast(val) },
            .Enum => from(if (comptime util.isDense(T)) @tagName(val) else @as(u32, @intFromEnum(val)), arena),
            else => {
                if (comptime util.isJsonRepresentable(T)) {
                    return .{ .string = try std.json.stringifyAlloc(arena, val, .{}) };
                }

                @compileError("TODO: " ++ @typeName(T));
            },
        };
    }

    pub fn into(self: Value, comptime T: type, arena: std.mem.Allocator) !T {
        if (comptime T == Value) {
            return self;
        }

        if (comptime std.meta.hasFn(T, "fromValue")) {
            return T.fromValue(self, arena);
        }

        if (comptime T == []const u8 or T == [:0]const u8) {
            return arena.dupeZ(u8, self.string);
        }

        return switch (@typeInfo(T)) {
            .Optional => |o| if (self == .null) null else try into(self, o.child, arena),
            .Bool => if (self.int) true else false,
            .Int, .ComptimeInt => @intCast(self.int),
            .Float, .ComptimeFloat => @floatCast(self.float),
            .Enum => if (comptime util.isDense(T)) std.meta.stringToEnum(T, self.string) orelse error.InvalidEnumTag else @enumFromInt(self.int),
            else => {
                if (comptime util.isJsonRepresentable(T)) {
                    return std.json.parseFromSliceLeaky(T, arena, self.string, .{
                        .allocate = .alloc_always,
                    });
                }

                @compileError("TODO: " ++ @typeName(T));
            },
        };
    }
};

pub const Blob = struct {
    bytes: []const u8,

    pub fn fromValue(val: Value, _: std.mem.Allocator) Blob {
        return Blob{ .bytes = val.blob };
    }

    pub fn toValue(self: Blob, _: std.mem.Allocator) Value {
        return .{ .blob = self.bytes };
    }
};
