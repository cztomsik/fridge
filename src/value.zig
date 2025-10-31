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
            .null => .null,
            .optional => if (val) |v| from(v, arena) else .null,
            .bool => .{ .int = if (val) 1 else 0 },
            .int, .comptime_int => .{ .int = @intCast(val) },
            .float, .comptime_float => .{ .float = @floatCast(val) },
            .@"enum" => from(if (comptime util.isDense(T)) @tagName(val) else @as(u32, @intFromEnum(val)), arena),
            else => {
                if (comptime util.isPacked(T)) |Int| {
                    const num: Int = @bitCast(val);
                    return .{ .int = @intCast(num) };
                }

                if (comptime util.isJsonRepresentable(T)) {
                    return .{ .string = try std.fmt.allocPrint(arena, "{f}", .{std.json.fmt(val, .{})}) };
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

        if (comptime util.isString(T)) {
            return arena.dupeZ(u8, self.string);
        }

        return switch (@typeInfo(T)) {
            .optional => |o| if (self == .null) null else try into(self, o.child, arena),
            .bool => if (self.int == 1) true else false,
            .int, .comptime_int => @intCast(self.int),
            .float, .comptime_float => @floatCast(self.float),
            .@"enum" => if (comptime util.isDense(T)) std.meta.stringToEnum(T, self.string) orelse error.InvalidEnumTag else @enumFromInt(self.int),
            else => {
                if (util.isPacked(T)) |Int| {
                    const num: Int = @intCast(self.int);
                    return @bitCast(num);
                }

                if (comptime util.isJsonRepresentable(T)) {
                    return std.json.parseFromSliceLeaky(T, arena, self.string, .{
                        .allocate = .alloc_always,
                        .ignore_unknown_fields = true,
                    });
                }

                @compileError("TODO: " ++ @typeName(T));
            },
        };
    }

    pub fn bind(self: Value, stmt: anytype, i: *usize) !void {
        try stmt.bind(i.*, self);
        i.* += 1;
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

test "packing" {
    const Flags = packed struct(u8) { a: bool, b: bool, c: bool, _: u5 = 0 };
    const flags: Flags = .{ .a = true, .b = false, .c = true };

    const val = try Value.from(flags, undefined);
    try std.testing.expect(val.int == 5);

    const flags2 = try val.into(Flags, undefined);
    try std.testing.expect(flags == flags2);

    const RGBA = packed struct(u32) { r: u8, g: u8, b: u8, a: u8 };
    const color: RGBA = .{ .r = 0xFF, .g = 0x80, .b = 0x40, .a = 0xCC };

    const val2 = try Value.from(color, undefined);
    try std.testing.expectEqual(val2.int, 0xCC4080FF);

    const color2 = try val2.into(RGBA, undefined);
    try std.testing.expect(color == color2);
}
