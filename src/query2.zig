const std = @import("std");
const Session = @import("session.zig").Session;
const Value = @import("value.zig").Value;
const Statement = @import("statement.zig").Statement;
const SqlBuf = @import("sql.zig").SqlBuf;
const util = @import("util.zig");

pub const RawQuery = Query(struct {});

pub fn Query(comptime T: type) type {
    return struct {
        db: *Session,
        parts: []const Part,

        const Q = @This();

        pub const Part = struct {
            kind: Kind,
            sql: []const u8,
            args: Args,

            pub const Kind = enum { empty, raw, SELECT, table, JOIN, @"LEFT JOIN", WHERE, @"OR WHERE", @"GROUP BY", HAVING, @"ORDER BY", LIMIT, OFFSET };
            pub const EMPTY: Part = part(.empty, "", {});

            fn part(kind: Kind, sql: []const u8, args: anytype) Part {
                return .{ .kind = kind, .sql = sql, .args = .from(args) };
            }

            pub fn raw(sql: []const u8, args: anytype) Part {
                return part(.raw, sql, args);
            }

            pub fn select(comptime sql: []const u8) Part {
                return part(.SELECT, sql, {});
            }

            pub fn table(comptime sql: []const u8) Part {
                return part(.table, sql, {});
            }

            pub const from = table;

            pub fn join(comptime sql: []const u8) Part {
                return part(.JOIN, sql, {});
            }

            pub fn leftJoin(comptime sql: []const u8) Part {
                return part(.@"LEFT JOIN", sql, {});
            }

            pub fn where(comptime sql: []const u8, args: anytype) Part {
                return part(.WHERE, if (@hasField(T, sql)) sql ++ " = ?" else sql, args);
            }

            pub fn ifWhere(cond: bool, comptime sql: []const u8, args: anytype) Part {
                return if (cond) where(sql, args) else EMPTY;
            }

            pub fn maybeWhere(comptime sql: []const u8, arg: anytype) Part {
                return if (arg) |v| where(sql, v) else EMPTY;
            }

            pub fn orWhere(comptime sql: []const u8, args: anytype) Part {
                return part(.@"OR WHERE", if (@hasField(T, sql)) sql ++ " = ?" else sql, args);
            }

            pub fn orIfWhere(cond: bool, comptime sql: []const u8, args: anytype) Part {
                return if (cond) orWhere(sql, args) else EMPTY;
            }

            pub fn orMaybeWhere(comptime sql: []const u8, arg: anytype) Part {
                return if (arg) |v| orWhere(sql, v) else EMPTY;
            }

            pub fn groupBy(comptime sql: []const u8) Part {
                return part(.@"GROUP BY", sql, {});
            }

            pub fn having(comptime sql: []const u8, args: anytype) Part {
                return part(.HAVING, sql, args);
            }

            pub fn orderBy(comptime sql: []const u8) Part {
                return part(.@"ORDER BY", sql, {});
            }

            pub fn limit(n: u32) Part {
                return part(.LIMIT, "?", n);
            }

            pub fn offset(n: u32) Part {
                return part(.OFFSET, "?", n);
            }
        };

        pub fn init(db: *Session, parts: []const Part) Q {
            return .{ .db = db, .parts = parts };
        }

        pub fn prepare(self: Q) !Statement {
            var buf = try SqlBuf.init(self.db.arena);
            try buf.append(self);

            var args: std.ArrayList(Value) = .empty;

            for (self.parts) |p| {
                switch (p.args) {
                    .none => {},
                    .one => |v| try args.append(self.db.arena, v),
                    .other => |o| try o.bind(o.ptr, self.db.arena, &args),
                }
            }

            return self.db.conn.prepare(buf.buf.items, args.items);
        }

        pub fn exec(self: Q) !void {
            var stmt = try self.prepare();
            defer stmt.deinit();

            try stmt.exec();
        }

        pub fn get(self: Q, comptime V: type) !?V {
            var stmt = try self.prepare();
            defer stmt.deinit();

            return if (try stmt.next(?V, self.db.arena)) |v| v else null;
        }

        pub fn pluck(self: Q, comptime V: type) ![]const V {
            var stmt = try self.prepare();
            defer stmt.deinit();

            var res = std.array_list.Managed(V).init(self.db.arena);
            errdefer res.deinit();

            while (try stmt.next(V, self.db.arena)) |v| {
                try res.append(v);
            }

            return res.toOwnedSlice();
        }

        pub fn fetchOne(self: Q, comptime R: type) !?R {
            var stmt = try self.prepare();
            defer stmt.deinit();

            return stmt.next(R, self.db.arena);
        }

        pub fn fetchAll(self: Q, comptime R: type) ![]const R {
            var stmt = try self.prepare();
            defer stmt.deinit();

            var res = std.array_list.Managed(R).init(self.db.arena);
            errdefer res.deinit();

            while (try stmt.next(R, self.db.arena)) |row| {
                try res.append(row);
            }

            return res.toOwnedSlice();
        }

        pub fn toSql(self: Q, buf: *SqlBuf) !void {
            if (!(self.parts.len > 0 and self.parts[0].kind == .SELECT)) {
                try buf.append("SELECT " ++ comptime util.columns(T));
            }

            var prev: Part.Kind = .empty;
            var has_table: bool = false;
            for (self.parts) |p| {
                if (@intFromEnum(p.kind) > @intFromEnum(Part.Kind.table) and !has_table) {
                    has_table = true;
                    try buf.append(" FROM " ++ comptime util.tableName(T));
                }

                switch (p.kind) {
                    .raw, .empty => {},
                    .table => {
                        has_table = true;
                        std.debug.assert(buf.buf.items.len > 1);
                        try buf.append(switch (std.ascii.toLower(buf.buf.items[1])) {
                            'e' => " FROM ", // SeLECT, DeLETE
                            'n' => " INTO ", // InSERT
                            else => " ",
                        });
                    },
                    else => {
                        const comma = switch (p.kind) {
                            .SELECT, .@"GROUP BY", .@"ORDER BY" => prev == p.kind,
                            else => false,
                        };

                        try buf.append(if (comma) ", " else switch (p.kind) {
                            .SELECT => "SELECT ",
                            .WHERE => if (prev == .WHERE or prev == .@"OR WHERE") " AND " else " WHERE ",
                            .@"OR WHERE" => if (prev == .WHERE or prev == .@"OR WHERE") " OR " else " WHERE ",
                            .HAVING => if (prev == .HAVING) " AND " else " HAVING ",
                            inline else => |t| " " ++ @tagName(t) ++ " ",
                        });
                    },
                }

                try buf.append(p.sql);
                prev = p.kind;
            }

            if (!has_table) {
                try buf.append(" FROM " ++ comptime util.tableName(T));
            }
        }
    };
}

pub const Args = union(enum) {
    none,
    one: Value,
    other: struct { ptr: *const anyopaque, bind: *const fn (*const anyopaque, std.mem.Allocator, *std.ArrayList(Value)) anyerror!void },

    inline fn isCoercable(comptime T: type) bool {
        return switch (@typeInfo(T)) {
            .null, .bool, .int, .comptime_int, .float, .comptime_float, .@"enum" => true,
            else => util.isString(T) or util.isPacked(T) != null,
        };
    }

    fn from(args: anytype) Args {
        if (comptime @TypeOf(args) == void) return .none;
        if (comptime isCoercable(@TypeOf(args))) return .{ .one = Value.from(args, undefined) catch unreachable };

        switch (@typeInfo(@TypeOf(args))) {
            .pointer => |p| {
                const H = struct {
                    fn bind(ptr: *const anyopaque, arena: std.mem.Allocator, argz: *std.ArrayList(Value)) anyerror!void {
                        const ref: *const p.child = @ptrCast(@alignCast(ptr));

                        if (comptime util.isTuple(p.child)) {
                            inline for (0..ref.len) |i| try argz.append(arena, try .from(ref[i], arena));
                        } else {
                            try argz.append(arena, try .from(ref, arena));
                        }
                    }
                };

                return .{ .other = .{ .ptr = args, .bind = H.bind } };
            },
            .@"struct" => if (comptime util.isTuple(@TypeOf(args)))
                @compileError("Expected &.{...} (pointer to tuple), got .{...} by value. Add & before the .{...}")
            else
                @compileError("Expected a pointer to " ++ @typeName(@TypeOf(args)) ++ ", use &val instead of val"),
            else => @compileError("Unexpected " ++ @typeName(@TypeOf(args)) ++ ". Use a primitive, &val, or &.{...}"),
        }
    }
};

const Person = struct {
    id: ?u32 = null,
    name: []const u8,
    age: u8,
};

const expectSql = @import("testing.zig").expectSql;

test "select" {
    var db: *Session = undefined;

    try expectSql(
        db.query(Person, &.{}),
        "SELECT id, name, age FROM Person",
    );

    try expectSql(
        db.query(Person, &.{.select("name")}),
        "SELECT name FROM Person",
    );

    try expectSql(
        db.query(Person, &.{ .select("name"), .select("age") }),
        "SELECT name, age FROM Person",
    );
}

test "join" {
    var db: *Session = undefined;

    try expectSql(
        db.query(Person, &.{.join("Address ON Person.id = Address.person_id")}),
        "SELECT id, name, age FROM Person JOIN Address ON Person.id = Address.person_id",
    );

    try expectSql(
        db.query(Person, &.{.leftJoin("Address ON Person.id = Address.person_id")}),
        "SELECT id, name, age FROM Person LEFT JOIN Address ON Person.id = Address.person_id",
    );
}

test "where" {
    var db: *Session = undefined;

    try expectSql(
        db.query(Person, &.{.where("name", "Alice")}),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        db.query(Person, &.{.where("age > ?", 18)}),
        "SELECT id, name, age FROM Person WHERE age > ?",
    );

    try expectSql(
        db.query(Person, &.{ .where("name", "Alice"), .where("age", 20) }),
        "SELECT id, name, age FROM Person WHERE name = ? AND age = ?",
    );

    try expectSql(
        db.query(Person, &.{.ifWhere(false, "name", "Alice")}),
        "SELECT id, name, age FROM Person",
    );

    try expectSql(
        db.query(Person, &.{.ifWhere(true, "name", "Alice")}),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        // Check that arg type is "flat" optional even for opt columns
        db.query(Person, &.{.maybeWhere("id", @as(?u32, null))}),
        "SELECT id, name, age FROM Person",
    );

    try expectSql(
        db.query(Person, &.{.maybeWhere("name", @as(?[]const u8, "Alice"))}),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );
}

test "orWhere" {
    var db: *Session = undefined;

    try expectSql(
        db.query(Person, &.{.orWhere("name", "Alice")}),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        db.query(Person, &.{.orWhere("age > ?", 18)}),
        "SELECT id, name, age FROM Person WHERE age > ?",
    );

    try expectSql(
        db.query(Person, &.{ .where("name", "Alice"), .orWhere("age", 20) }),
        "SELECT id, name, age FROM Person WHERE name = ? OR age = ?",
    );

    try expectSql(
        db.query(Person, &.{ .where("name", "Alice"), .orIfWhere(false, "age", 20) }),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        db.query(Person, &.{ .where("name", "Alice"), .orIfWhere(true, "age", 20) }),
        "SELECT id, name, age FROM Person WHERE name = ? OR age = ?",
    );

    try expectSql(
        db.query(Person, &.{ .where("name", "Alice"), .orMaybeWhere("age", @as(?u8, null)) }),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        db.query(Person, &.{ .where("name", "Alice"), .orMaybeWhere("age", @as(?u8, 20)) }),
        "SELECT id, name, age FROM Person WHERE name = ? OR age = ?",
    );
}

test "groupBy/having" {
    var db: *Session = undefined;

    try expectSql(
        db.query(Person, &.{.groupBy("name")}),
        "SELECT id, name, age FROM Person GROUP BY name",
    );

    try expectSql(
        db.query(Person, &.{ .groupBy("name"), .groupBy("age") }),
        "SELECT id, name, age FROM Person GROUP BY name, age",
    );

    try expectSql(
        db.query(Person, &.{ .groupBy("name"), .having("COUNT(*) > ?", 1) }),
        "SELECT id, name, age FROM Person GROUP BY name HAVING COUNT(*) > ?",
    );

    try expectSql(
        db.query(Person, &.{ .groupBy("name"), .having("COUNT(*) > ?", 1), .having("SUM(age) > ?", 50) }),
        "SELECT id, name, age FROM Person GROUP BY name HAVING COUNT(*) > ? AND SUM(age) > ?",
    );
}

test "orderBy" {
    var db: *Session = undefined;

    try expectSql(
        db.query(Person, &.{.orderBy("name asc")}),
        "SELECT id, name, age FROM Person ORDER BY name asc",
    );

    try expectSql(
        db.query(Person, &.{ .orderBy("name asc"), .orderBy("age desc") }),
        "SELECT id, name, age FROM Person ORDER BY name asc, age desc",
    );
}

test "limit/offset" {
    var db: *Session = undefined;

    try expectSql(
        db.query(Person, &.{.limit(10)}),
        "SELECT id, name, age FROM Person LIMIT ?",
    );

    try expectSql(
        db.query(Person, &.{.offset(10)}),
        "SELECT id, name, age FROM Person OFFSET ?",
    );

    try expectSql(
        db.query(Person, &.{ .limit(10), .offset(20) }),
        "SELECT id, name, age FROM Person LIMIT ? OFFSET ?",
    );
}
