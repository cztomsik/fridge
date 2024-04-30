pub fn isString(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .Pointer => |ptr| ptr.child == u8 or switch (@typeInfo(ptr.child)) {
            .Array => |arr| arr.child == u8,
            else => false,
        },
        else => false,
    };
}

pub fn isDense(comptime E: type) bool {
    for (@typeInfo(E).Enum.fields, 0..) |f, i| {
        if (f.value != i) return false;
    }

    return true;
}
