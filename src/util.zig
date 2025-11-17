const builtin = @import("builtin");
const std = @import("std");

pub const log = if (builtin.is_test) struct { // zig build test captures stderr and there seems to be no f* way to disable it
    pub fn debug(comptime fmt: []const u8, args: anytype) void {
        std.debug.print("debug: " ++ fmt ++ "\n", args);
    }

    pub fn err(comptime fmt: []const u8, args: anytype) void {
        std.debug.print("err: " ++ fmt ++ "\n", args);
    }
} else std.log.scoped(.fridge);

pub fn isSimpleExpr(expr: []const u8) bool {
    return std.mem.indexOfScalar(u8, expr, '(') == null and
        std.mem.indexOf(u8, expr, "->") == null and
        std.mem.indexOf(u8, expr, "AND") == null and
        std.mem.indexOf(u8, expr, "OR") == null;
}

// TODO: if we ever want to remap fields, this is the place
pub fn ColType(comptime T: type, comptime expr: []const u8) type {
    if (@hasField(T, expr)) {
        return @FieldType(T, expr);
    }

    const inferable = expr[expr.len - 1] == '?' and isSimpleExpr(expr);

    if (inferable) {
        if (std.mem.indexOfScalar(u8, expr, ' ')) |i| {
            return @FieldType(T, expr[0..i]);
        }
    }

    @compileError("Cannot infer arg type " ++ expr ++ "; You might need to use the xxxRaw() counterpart");
}

pub fn MaybeColType(comptime T: type, comptime field_name: []const u8) type {
    const Col = ColType(T, field_name);

    return switch (@typeInfo(Col)) {
        .optional => Col,
        else => ?Col,
    };
}

pub fn Id(comptime T: type) type {
    const Col = ColType(T, "id");

    return switch (@typeInfo(Col)) {
        .optional => |o| o.child,
        else => Col,
    };
}

pub fn tableName(comptime T: type) []const u8 {
    return comptime brk: {
        if (@hasDecl(T, "sql_table_name")) break :brk T.sql_table_name;
        const s = @typeName(T);
        const i = std.mem.lastIndexOfScalar(u8, s, '.').?;
        break :brk s[i + 1 ..];
    };
}

pub fn columns(comptime T: type) []const u8 {
    return comptime brk: {
        var res: []const u8 = "";

        for (@typeInfo(T).@"struct".fields) |f| {
            if (res.len > 0) res = res ++ ", ";
            res = res ++ f.name;
        }

        break :brk res;
    };
}

pub fn placeholders(comptime T: type) []const u8 {
    return comptime brk: {
        var res: []const u8 = "";

        for (@typeInfo(T).@"struct".fields) |_| {
            if (res.len > 0) res = res ++ ", ";
            res = res ++ "?";
        }

        break :brk res;
    };
}

pub fn setters(comptime T: type) []const u8 {
    return comptime brk: {
        var res: []const u8 = "";

        for (@typeInfo(T).@"struct".fields) |f| {
            if (res.len > 0) res = res ++ ", ";
            res = res ++ f.name ++ " = ?";
        }

        break :brk res;
    };
}

pub fn isTuple(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .@"struct" => |s| s.is_tuple,
        else => false,
    };
}

pub fn isString(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .pointer => |ptr| ptr.child == u8 or switch (@typeInfo(ptr.child)) {
            .array => |arr| arr.child == u8,
            else => false,
        },
        else => false,
    };
}

pub fn isDense(comptime E: type) bool {
    for (@typeInfo(E).@"enum".fields, 0..) |f, i| {
        if (f.value != i) return false;
    }

    return true;
}

pub fn isPacked(comptime T: type) ?type {
    return switch (@typeInfo(T)) {
        .@"struct" => |s| s.backing_integer,
        else => null,
    };
}

pub fn isJsonRepresentable(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .array, .@"struct", .@"union" => true,
        .pointer => |p| p.size == .slice,
        else => false,
    };
}

pub fn checkFields(comptime T: type, comptime D: type) void {
    comptime {
        outer: for (@typeInfo(D).@"struct".fields) |f| {
            for (@typeInfo(T).@"struct".fields) |f2| {
                if (std.mem.eql(u8, f.name, f2.name)) {
                    if (isAssignableTo(f.type, f2.type)) {
                        continue :outer;
                    }

                    @compileError(
                        "Type mismatch for field " ++ f.name ++
                            " found:" ++ @typeName(f.type) ++
                            " expected:" ++ @typeName(f2.type),
                    );
                }
            }

            @compileError("Unknown field " ++ f.name);
        }
    }
}

pub fn isAssignableTo(comptime A: type, B: type) bool {
    if (A == B) return true;
    if (isString(A) and isString(B)) return true;

    switch (@typeInfo(A)) {
        .comptime_int => if (@typeInfo(B) == .int) return true,
        .comptime_float => if (@typeInfo(B) == .float) return true,
        else => {},
    }

    switch (@typeInfo(B)) {
        .optional => |opt| {
            if (A == @TypeOf(null)) return true;
            if (isAssignableTo(A, opt.child)) return true;
        },
        else => {},
    }

    return false;
}
