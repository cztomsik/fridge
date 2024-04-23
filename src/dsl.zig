const std = @import("std");

/// Create a raw SQL fragment.
pub fn raw(comptime sql: []const u8, bindings: anytype) Raw(sql, @TypeOf(bindings)) {
    return .{ .bindings = bindings };
}

/// Create a query.
pub fn query(comptime T: type) Query(T, T, struct {}, void) {
    return .{
        .kind = .select,
        .whr = void{},
        .order_by = &.{},
        .lim = -1,
        .off = -1,
    };
}

fn Raw(comptime raw_sql: []const u8, comptime T: type) type {
    return struct {
        bindings: T,

        pub fn sql(_: *const @This(), buf: *std.ArrayList(u8)) !void {
            try buf.appendSlice(raw_sql);
        }

        pub fn bind(self: *const @This(), binder: anytype) !void {
            if (comptime T == void) return;

            inline for (@typeInfo(T).Struct.fields) |f| {
                try binder.bind(@field(self.bindings, f.name));
            }
        }
    };
}

fn Query(comptime T: type, comptime R: type, comptime D: type, comptime W: type) type {
    return struct {
        pub const Row = R;

        kind: union(enum) {
            select,
            insert: D,
            update: D,
            delete,
        },
        whr: W,
        order_by: []const u8,
        lim: i32,
        off: i32,

        const Self = @This();

        pub fn where(self: Self, criteria: anytype) Query(T, R, D, Where(W, " AND ", @TypeOf(criteria))) {
            return .{
                .kind = self.kind,
                .whr = .{ .a = self.whr, .b = criteria },
                .order_by = self.order_by,
                .lim = self.lim,
                .off = self.off,
            };
        }

        pub fn orWhere(self: Self, criteria: anytype) Query(T, R, D, Where(W, " OR ", @TypeOf(criteria))) {
            return .{
                .kind = self.kind,
                .whr = .{ .a = self.whr, .b = criteria },
                .order_by = self.order_by,
                .lim = self.lim,
                .off = self.off,
            };
        }

        // orWhere, ifWhere, optWhere, ???

        pub fn orderBy(self: Self, col: std.meta.FieldEnum(T), ord: enum { asc, desc }) Self {
            return self.orderByRaw(switch (col) {
                inline else => |c| switch (ord) {
                    inline else => |o| @tagName(c) ++ " " ++ @tagName(o),
                },
            });
        }

        pub fn orderByRaw(self: Self, order_by: []const u8) Self {
            var copy = self;
            copy.order_by = order_by;
            return copy;
        }

        pub fn limit(self: Self, n: i32) Self {
            var copy = self;
            copy.lim = n;
            return copy;
        }

        pub fn offset(self: Self, i: i32) Self {
            var copy = self;
            copy.offset = i;
            return copy;
        }

        pub fn select(self: Self, comptime sel: []const std.meta.FieldEnum(T)) Query(T, Select(T, sel), D, W) {
            return .{
                .kind = .select,
                .whr = self.whr,
                .order_by = self.order_by,
                .lim = self.lim,
                .off = self.off,
            };
        }

        pub fn count(self: Self) Query(T, struct { @"COUNT(*)": u64 }, D, W) {
            return .{
                .kind = .select,
                .whr = self.whr,
                .order_by = &.{},
                .lim = -1,
                .off = -1,
            };
        }

        pub fn insert(self: Self, data: anytype) Query(T, R, @TypeOf(data), W) {
            comptime checkFields(T, @TypeOf(data));

            return .{
                .kind = .{ .insert = data },
                .whr = self.whr,
                .order_by = &.{},
                .lim = self.lim,
                .off = self.off,
            };
        }

        pub fn update(self: Self, data: anytype) Query(T, R, @TypeOf(data), W) {
            comptime checkFields(T, @TypeOf(data));

            return .{
                .kind = .{ .update = data },
                .whr = self.whr,
                .order_by = &.{},
                .lim = self.lim,
                .off = self.off,
            };
        }

        pub fn delete(self: Self) Self {
            var copy = self;
            copy.kind = .delete;
            return copy;
        }

        pub fn sql(self: *const Self, buf: *std.ArrayList(u8)) !void {
            try buf.appendSlice(switch (self.kind) {
                .select => comptime "SELECT " ++ fields(R) ++ " FROM ",
                .insert => "INSERT INTO ",
                .update => "UPDATE ",
                .delete => "DELETE FROM ",
            });

            try buf.appendSlice(tableName(T));

            switch (self.kind) {
                .insert => try buf.appendSlice(comptime "(" ++ fields(D) ++ ") VALUES (" ++ placeholders(D) ++ ")"),
                .update => try buf.appendSlice(comptime " SET " ++ setters(D)),
                else => {},
            }

            if (comptime W != void) {
                try buf.appendSlice(" WHERE ");
                try self.whr.sql(buf);

                // TODO: find a better way
                if (std.mem.endsWith(u8, buf.items, " WHERE ")) {
                    buf.items.len -= 7;
                }
            }

            if (self.order_by.len > 0) {
                try buf.appendSlice(" ORDER BY ");
                try buf.appendSlice(self.order_by);
            }

            if (self.lim >= 0) {
                try buf.appendSlice(" LIMIT ?");
            }

            if (self.off >= 0) {
                try buf.appendSlice(" OFFSET ?");
            }
        }

        pub fn bind(self: *const Self, binder: anytype) !void {
            switch (self.kind) {
                inline .insert, .update => |data| {
                    inline for (@typeInfo(D).Struct.fields) |f| {
                        try binder.bind(@field(data, f.name));
                    }
                },
                else => {},
            }

            if (comptime W != void) {
                try self.whr.bind(binder);
            }

            if (self.lim >= 0) {
                try binder.bind(self.lim);
            }

            if (self.off >= 0) {
                try binder.bind(self.off);
            }
        }
    };
}

pub fn Where(comptime A: type, comptime op: []const u8, comptime B: type) type {
    return struct {
        a: A,
        b: B,

        fn sql(self: *const @This(), buf: *std.ArrayList(u8)) !void {
            if (comptime A != void) {
                try sqlPart(self.a, buf);
                try buf.appendSlice(op);
            }

            try sqlPart(self.b, buf);
        }

        fn sqlPart(part: anytype, buf: *std.ArrayList(u8)) !void {
            if (comptime @hasDecl(@TypeOf(part), "sql")) {
                return part.sql(buf);
            }

            var n: usize = 0;

            inline for (@typeInfo(@TypeOf(part)).Struct.fields) |f| {
                const opt = comptime f.name[0] == '?';
                const suffix = comptime if (std.mem.indexOfScalar(u8, f.name, ' ') != null) " ?" else " = ?";
                const expr = comptime f.name[@intFromBool(opt)..] ++ suffix;

                if (if (comptime opt) @field(part, f.name) != null else true) {
                    if (n > 0) try buf.appendSlice(" AND ");
                    defer n += 1;

                    try buf.appendSlice(expr);
                }
            }
        }

        pub fn bind(self: *const @This(), binder: anytype) !void {
            if (comptime A != void) {
                try bindPart(self.a, binder);
            }

            try bindPart(self.b, binder);
        }

        fn bindPart(part: anytype, binder: anytype) !void {
            if (comptime @hasDecl(@TypeOf(part), "bind")) {
                return part.bind(binder);
            }

            inline for (@typeInfo(@TypeOf(part)).Struct.fields) |f| {
                const opt = comptime f.name[0] == '?';

                if (if (comptime opt) @field(part, f.name) != null else true) {
                    try binder.bind(@field(part, f.name));
                }
            }
        }
    };
}

pub fn Select(comptime T: type, comptime sel: []const std.meta.FieldEnum(T)) type {
    var arr: [sel.len]std.builtin.Type.StructField = undefined;
    for (sel, 0..) |e, i| arr[i] = std.meta.fieldInfo(T, e);

    return @Type(.{ .Struct = .{
        .layout = .auto,
        .is_tuple = false,
        .fields = &arr,
        .decls = &.{},
    } });
}

fn tableName(comptime T: type) []const u8 {
    return comptime brk: {
        const s = @typeName(T);
        const i = std.mem.lastIndexOfScalar(u8, s, '.').?;
        break :brk s[i + 1 ..];
    };
}

fn fields(comptime T: type) []const u8 {
    return comptime brk: {
        var res: []const u8 = "";

        for (@typeInfo(T).Struct.fields) |f| {
            if (res.len > 0) res = res ++ ", ";
            res = res ++ f.name;
        }

        break :brk res;
    };
}

fn placeholders(comptime T: type) []const u8 {
    return comptime brk: {
        var res: []const u8 = "";

        for (@typeInfo(T).Struct.fields) |_| {
            if (res.len > 0) res = res ++ ", ";
            res = res ++ "?";
        }

        break :brk res;
    };
}

fn setters(comptime T: type) []const u8 {
    return comptime brk: {
        var res: []const u8 = "";

        for (@typeInfo(T).Struct.fields) |f| {
            if (res.len > 0) res = res ++ ", ";
            res = res ++ f.name ++ " = ?";
        }

        break :brk res;
    };
}

fn checkFields(comptime T: type, comptime D: type) void {
    comptime {
        outer: for (@typeInfo(D).Struct.fields) |f| {
            for (@typeInfo(T).Struct.fields) |f2| {
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

fn isAssignableTo(comptime A: type, B: type) bool {
    if (A == B) return true;
    if (isString(A) and isString(B)) return true;

    switch (@typeInfo(A)) {
        .ComptimeInt => if (@typeInfo(B) == .Int) return true,
        .ComptimeFloat => if (@typeInfo(B) == .Float) return true,
        else => {},
    }

    switch (@typeInfo(B)) {
        .Optional => |opt| {
            if (A == @TypeOf(null)) return true;
            if (isAssignableTo(A, opt.child)) return true;
        },
        else => {},
    }

    return false;
}

pub fn isString(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .Pointer => |ptr| ptr.child == u8 or switch (@typeInfo(ptr.child)) {
            .Array => |arr| arr.child == u8,
            else => false,
        },
        else => false,
    };
}

fn expectSql(q: anytype, sql: []const u8) !void {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    try q.sql(&buf);
    try std.testing.expectEqualStrings(sql, buf.items);
}

const Person = struct {
    id: u32,
    name: []const u8,
    age: u8,
};

test "query" {
    try expectSql(
        query(Person),
        "SELECT id, name, age FROM Person",
    );

    try expectSql(
        query(Person).where(.{ .name = "Alice" }),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        query(Person).where(.{ .name = "Alice" }).orWhere(.{ .age = 20 }),
        "SELECT id, name, age FROM Person WHERE name = ? OR age = ?",
    );

    try expectSql(
        query(Person)
            .where(raw("name = ?", .{"Alice"}))
            .orWhere(raw("age > ?", .{20})),
        "SELECT id, name, age FROM Person WHERE name = ? OR age > ?",
    );

    try expectSql(
        query(Person).where(.{ .@"age >=" = 20 }),
        "SELECT id, name, age FROM Person WHERE age >= ?",
    );

    try expectSql(
        query(Person).where(.{ .@"?age" = null }),
        "SELECT id, name, age FROM Person",
    );

    try expectSql(
        query(Person).where(.{ .@"?age" = null, .name = "Alice" }),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        query(Person).where(.{ .@"?age" = @as(?u32, 20) }),
        "SELECT id, name, age FROM Person WHERE age = ?",
    );

    try expectSql(
        query(Person).orderBy(.name, .asc),
        "SELECT id, name, age FROM Person ORDER BY name asc",
    );

    try expectSql(
        query(Person).limit(10).limit(20),
        "SELECT id, name, age FROM Person LIMIT ?",
    );
}

test "query.select()" {
    const q = query(Person).select(&.{ .name, .age });

    try expectSql(q, "SELECT name, age FROM Person");

    try std.testing.expectEqualDeep(
        std.meta.fieldNames(struct { name: []const u8, age: u8 }),
        std.meta.fieldNames(@TypeOf(q).Row),
    );
}

test "query.count()" {
    const q = query(Person).count();

    try expectSql(q, "SELECT COUNT(*) FROM Person");

    try std.testing.expectEqualDeep(
        std.meta.fieldNames(struct { @"COUNT(*)": u64 }),
        std.meta.fieldNames(@TypeOf(q).Row),
    );
}

test "insert" {
    try expectSql(query(Person).insert(.{}), "INSERT INTO Person() VALUES ()");

    try expectSql(
        query(Person).insert(.{ .name = "Alice", .age = 20 }),
        "INSERT INTO Person(name, age) VALUES (?, ?)",
    );
}

test "update" {
    try expectSql(query(Person).update(.{ .name = "Alice" }), "UPDATE Person SET name = ?");

    try expectSql(
        query(Person).where(.{ .age = 20 }).update(.{ .name = "Alice" }),
        "UPDATE Person SET name = ? WHERE age = ?",
    );

    try expectSql(
        query(Person).where(.{ .age = 20 }).orWhere(.{ .name = "Bob" }).update(.{ .name = "Alice" }),
        "UPDATE Person SET name = ? WHERE age = ? OR name = ?",
    );

    try expectSql(
        query(Person).where(.{ .age = 20 }).update(.{ .name = "Alice", .age = 21 }),
        "UPDATE Person SET name = ?, age = ? WHERE age = ?",
    );
}

test "delete" {
    try expectSql(query(Person).delete(), "DELETE FROM Person");

    try expectSql(
        query(Person).where(.{ .age = 20 }).delete(),
        "DELETE FROM Person WHERE age = ?",
    );

    try expectSql(
        query(Person).where(.{ .age = 20 }).orWhere(.{ .name = "Bob" }).delete(),
        "DELETE FROM Person WHERE age = ? OR name = ?",
    );
}
