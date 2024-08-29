const std = @import("std");
const util = @import("util.zig");
const Value = @import("value.zig").Value;
const Session = @import("session.zig").Session;
const Statement = @import("statement.zig").Statement;

pub fn Query(comptime T: type, comptime R: type) type {
    return struct {
        session: *Session,
        tail: *const RawPart = &.{
            .kind = .from,
            .sql = util.tableName(T),
        },

        const Q = @This();
        const Col = std.meta.FieldEnum(T);

        /// Change the return type of the query.
        pub fn as(self: Q, comptime R2: type) Query(T, R2) {
            return .{
                .session = self.session,
                .tail = self.tail,
            };
        }

        pub fn select(self: Q, comptime col: Col) Q {
            return self.selectRaw(@tagName(col));
        }

        pub fn selectRaw(self: Q, columns: []const u8) Q {
            return self.append(.select, columns, .{});
        }

        pub fn fromRaw(self: Q, from: []const u8) Q {
            return self.append(.from, from, .{});
        }

        pub fn joinRaw(self: Q, join: []const u8) Q {
            return self.append(.join, join, .{});
        }

        pub fn where(self: Q, comptime col: Col, val: std.meta.FieldType(T, col)) Q {
            return self.whereRaw(@tagName(col) ++ " = ?", .{val});
        }

        pub fn whereRaw(self: Q, whr: []const u8, args: anytype) Q {
            return self.append(.and_where, whr, args);
        }

        pub fn orWhere(self: Q, comptime col: Col, val: std.meta.FieldType(T, col)) Q {
            return self.orWhereRaw(@tagName(col) ++ " = ?", .{val});
        }

        pub fn orWhereRaw(self: Q, whr: []const u8, args: anytype) Q {
            return self.append(.or_where, whr, args);
        }

        pub fn groupBy(self: Q, col: Col) Q {
            return self.groupByRaw(@tagName(col));
        }

        pub fn groupByRaw(self: Q, group_by: []const u8) Q {
            return self.append(.group_by, group_by, .{});
        }

        pub fn orderBy(self: Q, col: Col, ord: enum { asc, desc }) Q {
            return self.orderByRaw(switch (col) {
                inline else => |c| switch (ord) {
                    inline else => |o| @tagName(c) ++ " " ++ @tagName(o),
                },
            });
        }

        pub fn orderByRaw(self: Q, order_by: []const u8) Q {
            return self.append(.order_by, order_by, .{});
        }

        pub fn limit(self: Q, n: i32) Q {
            return self.append(.limit, " LIMIT ?", .{n});
        }

        pub fn offset(self: Q, i: i32) Q {
            return self.append(.offset, " OFFSET ?", .{i});
        }

        pub fn value(self: Q, comptime col: Col) !?std.meta.FieldType(T, col) {
            return self.valueRaw(std.meta.FieldType(T, col), @tagName(col));
        }

        pub fn valueRaw(self: Q, comptime V: type, expr: []const u8) !?V {
            var stmt = try self.selectRaw(expr).limit(1).prepare();
            defer stmt.deinit();

            if (try stmt.next(struct { V }, self.session.arena)) |row| {
                return row[0];
            }

            return null;
        }

        pub fn exists(self: Q) !bool {
            return try self.valueRaw(bool, "1") orelse false;
        }

        pub fn count(self: Q, comptime col: Col) !u64 {
            return (try self.valueRaw(u64, "COUNT(" ++ @tagName(col) ++ ")")).?;
        }

        pub fn min(self: Q, comptime col: Col) !?std.meta.FieldType(T, col) {
            return self.valueRaw(std.meta.FieldType(T, col), "MIN(" ++ @tagName(col) ++ ")");
        }

        pub fn max(self: Q, comptime col: Col) !?std.meta.FieldType(T, col) {
            return self.valueRaw(std.meta.FieldType(T, col), "MAX(" ++ @tagName(col) ++ ")");
        }

        pub fn avg(self: Q, comptime col: Col) !?std.meta.FieldType(T, col) {
            return self.valueRaw(std.meta.FieldType(T, col), "AVG(" ++ @tagName(col) ++ ")");
        }

        pub fn exec(self: Q) !void {
            var stmt = try self.prepare();
            defer stmt.deinit();

            try stmt.exec();
        }

        pub fn find(self: Q, id: std.meta.FieldType(T, .id)) !?R {
            return self.findBy(.id, id);
        }

        pub fn findBy(self: Q, comptime col: Col, val: std.meta.FieldType(T, col)) !?R {
            return self.where(col, val).findFirst();
        }

        pub fn findFirst(self: Q) !?R {
            var stmt = try self.limit(1).prepare();
            defer stmt.deinit();

            return stmt.next(R, self.session.arena);
        }

        pub fn findAll(self: Q) ![]const R {
            var stmt = try self.prepare();
            defer stmt.deinit();

            var res = std.ArrayList(R).init(self.session.arena);
            errdefer res.deinit();

            while (try stmt.next(R, self.session.arena)) |row| {
                try res.append(row);
            }

            return res.toOwnedSlice();
        }

        pub fn insert(self: Q, data: anytype) !void {
            return self.asInsert().values(data).exec();
        }

        pub fn asInsert(self: Q) Q {
            return self.append(.insert, util.tableName(T), .{});
        }

        pub fn values(self: Q, data: anytype) Q {
            comptime util.checkFields(T, @TypeOf(data));

            return self.append(.values, comptime "(" ++ util.columns(@TypeOf(data)) ++ ") VALUES (" ++ util.placeholders(@TypeOf(data)) ++ ")", data);
        }

        pub fn onConflictRaw(self: Q, sql: []const u8, args: anytype) Q {
            return self.append(.on_conflict, sql, args);
        }

        pub fn update(self: Q, data: anytype) !void {
            return self.asUpdate().setAll(data).exec();
        }

        pub fn asUpdate(self: Q) Q {
            return self.append(.update, util.tableName(T), .{});
        }

        pub fn set(self: Q, comptime col: Col, val: std.meta.FieldType(T, col)) Q {
            return self.setRaw(@tagName(col) ++ " = ?", .{val});
        }

        pub fn setRaw(self: Q, sql: []const u8, args: anytype) Q {
            return self.append(.set, sql, args);
        }

        pub fn setAll(self: Q, data: anytype) Q {
            comptime util.checkFields(T, @TypeOf(data));

            return self.append(.set, util.setters(@TypeOf(data)), data);
        }

        pub fn delete(self: Q) !void {
            return self.asDelete().exec();
        }

        pub fn asDelete(self: Q) Q {
            return self.append(.delete, util.tableName(T), .{});
        }

        pub fn prepare(self: Q) !Statement {
            var compiled = try self.compile();
            var stmt = try self.session.prepare(try compiled.sql(), .{});
            errdefer stmt.deinit();

            try compiled.bind(&stmt);
            return stmt;
        }

        fn compile(self: Q) !Compiled {
            return Compiled.compile(
                self.tail,
                comptime &.{ .kind = .select, .sql = util.columns(T) },
                self.session.arena,
            );
        }

        fn append(self: Q, kind: RawPart.Kind, sql: []const u8, args: anytype) Q {
            const part = self.session.arena.create(RawPart) catch @panic("OOM");
            part.* = .{
                .prev = self.tail,
                .kind = kind,
                .sql = sql,
            };

            const fields = @typeInfo(@TypeOf(args)).Struct.fields;
            if (comptime fields.len > 0) {
                const vals = self.session.arena.alloc(Value, fields.len) catch @panic("OOM");

                inline for (fields, 0..) |f, i| {
                    vals[i] = Value.from(@field(args, f.name), self.session.arena) catch @panic("OOM");
                }

                part.args = vals;
            }

            var copy = self;
            copy.tail = part;
            return copy;
        }
    };
}

const Compiled = struct {
    parts: std.ArrayList(*const RawPart),

    fn compile(tail: *const RawPart, default_select: *const RawPart, arena: std.mem.Allocator) !Compiled {
        var parts = try std.ArrayList(*const RawPart).initCapacity(arena, 16);
        errdefer parts.deinit();

        var exclude: u32 = 0;
        var rest: ?*const RawPart = tail;
        while (rest) |p| : (rest = p.prev) {
            if (exclude & RawPart.maskBit(p.kind) > 0) continue;
            exclude |= p.mask();

            try parts.append(p);
        }

        if (exclude & RawPart.maskBit(.default) == 0) {
            try parts.append(default_select);
        }

        std.sort.insertion(*const RawPart, parts.items, {}, RawPart.cmp);
        return .{ .parts = parts };
    }

    pub fn deinit(self: *Compiled) void {
        self.parts.deinit();
    }

    pub fn sql(self: *Compiled) ![]const u8 {
        var buf = std.ArrayList(u8).init(self.parts.allocator);
        defer buf.deinit();

        try self.writeSql(&buf);
        return buf.toOwnedSlice();
    }

    pub fn writeSql(self: *Compiled, buf: *std.ArrayList(u8)) !void {
        var pos: i32 = -1;

        for (self.parts.items) |part| {
            if (part.order() > pos) {
                if (part.prefix()) |p| try buf.appendSlice(p);
                pos = part.order();
            } else {
                if (part.separator()) |s| try buf.appendSlice(s);
            }

            try buf.appendSlice(part.sql);
        }
    }

    fn bind(self: *Compiled, stmt: *Statement) !void {
        var i: usize = 0;

        for (self.parts.items) |part| {
            for (part.args) |arg| {
                try stmt.bind(i, arg);
                i += 1;
            }
        }
    }
};

const RawPart = struct {
    prev: ?*const RawPart = null,
    kind: Kind,
    sql: []const u8,
    args: []const Value = &.{},

    const Kind = enum { default, select, from, insert, values, on_conflict, update, set, delete, join, and_where, or_where, group_by, order_by, limit, offset };

    fn order(self: RawPart) i32 {
        return switch (self.kind) {
            .or_where => @intFromEnum(Kind.and_where),
            inline else => |t| @intFromEnum(t),
        };
    }

    // This works a bit like bloom filter, so parts later in the list can
    // replace or even discard previous parts.
    fn mask(self: RawPart) u32 {
        return switch (self.kind) {
            .select => maskBit(.default),
            inline .insert, .update, .delete => maskBit(.default) | maskBit(.select) | maskBit(.from),
            inline .from, .limit, .offset => |t| maskBit(t),
            else => 0,
        };
    }

    fn maskBit(kind: Kind) u32 {
        return @as(u32, 1) << @intFromEnum(kind);
    }

    fn prefix(self: RawPart) ?[]const u8 {
        return switch (self.kind) {
            .values, .limit, .offset => null,
            .select => "SELECT ",
            .from => " FROM ",
            .insert => "INSERT INTO ",
            .update => "UPDATE ",
            .set => " SET ",
            .on_conflict => " ON CONFLICT ",
            .delete => "DELETE FROM ",
            .and_where, .or_where => " WHERE ",
            .group_by => " GROUP BY ",
            .order_by => " ORDER BY ",
            else => " ",
        };
    }

    fn separator(self: RawPart) ?[]const u8 {
        return switch (self.kind) {
            .select, .set, .group_by, .order_by => ", ",
            .on_conflict => " ON CONFLICT ",
            .and_where => " AND ",
            .or_where => " OR ",
            else => null,
        };
    }

    fn cmp(_: void, a: *const RawPart, b: *const RawPart) bool {
        return a.order() <= b.order();
    }
};

const Person = struct {
    id: u32,
    name: []const u8,
    age: u8,
};

var _arena = std.heap.ArenaAllocator.init(std.testing.allocator);
var db: Session = .{ .arena = _arena.allocator(), .conn = undefined };

fn expectSql(q: Query(Person, Person), sql: []const u8) !void {
    defer _ = _arena.reset(.free_all);

    var compiled = try q.compile();
    try std.testing.expectEqualStrings(sql, try compiled.sql());
}

test "query" {
    try expectSql(
        db.query(Person),
        "SELECT id, name, age FROM Person",
    );
}

test "query.select()" {
    try expectSql(
        db.query(Person).select(.name),
        "SELECT name FROM Person",
    );

    try expectSql(
        db.query(Person).select(.name).select(.age),
        "SELECT name, age FROM Person",
    );

    try expectSql(
        db.query(Person).selectRaw("name, age"),
        "SELECT name, age FROM Person",
    );
}

test "query.fromRaw()" {
    try expectSql(
        db.query(Person).fromRaw("Person p"),
        "SELECT id, name, age FROM Person p",
    );
}

test "query.join()" {
    try expectSql(
        db.query(Person).joinRaw("JOIN Address ON Person.id = Address.person_id"),
        "SELECT id, name, age FROM Person JOIN Address ON Person.id = Address.person_id",
    );
}

test "query.where()" {
    try expectSql(
        db.query(Person).where(.name, "Alice"),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        db.query(Person).where(.name, "Alice").where(.age, 20),
        "SELECT id, name, age FROM Person WHERE name = ? AND age = ?",
    );

    try expectSql(
        db.query(Person).whereRaw("name = ?", .{"Alice"}).whereRaw("age > ?", .{20}),
        "SELECT id, name, age FROM Person WHERE name = ? AND age > ?",
    );
}

test "query.orWhere()" {
    try expectSql(
        db.query(Person).orWhere(.name, "Alice"),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        db.query(Person).where(.name, "Alice").orWhere(.age, 20),
        "SELECT id, name, age FROM Person WHERE name = ? OR age = ?",
    );

    try expectSql(
        db.query(Person).whereRaw("name = ?", .{"Alice"}).orWhereRaw("age > ?", .{20}),
        "SELECT id, name, age FROM Person WHERE name = ? OR age > ?",
    );
}

// TODO: select()
test "query.groupBy()" {
    try expectSql(
        db.query(Person).groupBy(.name),
        "SELECT id, name, age FROM Person GROUP BY name",
    );

    try expectSql(
        db.query(Person).groupBy(.name).groupBy(.age),
        "SELECT id, name, age FROM Person GROUP BY name, age",
    );
}

test "query.orderBy()" {
    try expectSql(
        db.query(Person).orderBy(.name, .asc),
        "SELECT id, name, age FROM Person ORDER BY name asc",
    );

    try expectSql(
        db.query(Person).orderBy(.name, .asc).orderBy(.age, .desc),
        "SELECT id, name, age FROM Person ORDER BY name asc, age desc",
    );
}

test "query.limit()" {
    try expectSql(
        db.query(Person).limit(10).limit(20),
        "SELECT id, name, age FROM Person LIMIT ?",
    );
}

test "query.offset()" {
    try expectSql(
        db.query(Person).offset(10).offset(20),
        "SELECT id, name, age FROM Person OFFSET ?",
    );
}

test "query.insert()" {
    try expectSql(
        db.query(Person).asInsert().values(.{}),
        "INSERT INTO Person() VALUES ()",
    );

    try expectSql(
        db.query(Person).asInsert().values(.{ .name = "Alice", .age = 20 }),
        "INSERT INTO Person(name, age) VALUES (?, ?)",
    );
}

test "query.onConflictRaw()" {
    try expectSql(
        db.query(Person).asInsert().values(.{ .name = "Alice", .age = 20 }).onConflictRaw("DO NOTHING", .{}),
        "INSERT INTO Person(name, age) VALUES (?, ?) ON CONFLICT DO NOTHING",
    );
}

test "query.update()" {
    try expectSql(
        db.query(Person).asUpdate().setAll(.{ .name = "Alice" }),
        "UPDATE Person SET name = ?",
    );

    try expectSql(
        db.query(Person).where(.age, 20).asUpdate().setAll(.{ .name = "Alice" }),
        "UPDATE Person SET name = ? WHERE age = ?",
    );

    try expectSql(
        db.query(Person).where(.age, 20).orWhere(.name, "Bob").asUpdate().setAll(.{ .name = "Alice" }),
        "UPDATE Person SET name = ? WHERE age = ? OR name = ?",
    );

    try expectSql(
        db.query(Person).where(.age, 20).asUpdate().setAll(.{ .name = "Alice", .age = 21 }),
        "UPDATE Person SET name = ?, age = ? WHERE age = ?",
    );
}

test "query.set()" {
    try expectSql(
        db.query(Person).asUpdate().set(.age, 21),
        "UPDATE Person SET age = ?",
    );

    try expectSql(
        db.query(Person).asUpdate().setAll(.{ .name = "Alice" }).set(.age, 21),
        "UPDATE Person SET name = ?, age = ?",
    );

    try expectSql(
        db.query(Person).asUpdate().setAll(.{ .name = "Alice" }).setRaw("age = age + ?", .{1}),
        "UPDATE Person SET name = ?, age = age + ?",
    );
}

test "query.delete()" {
    try expectSql(
        db.query(Person).asDelete(),
        "DELETE FROM Person",
    );

    try expectSql(
        db.query(Person).where(.age, 20).asDelete(),
        "DELETE FROM Person WHERE age = ?",
    );

    try expectSql(
        db.query(Person).where(.age, 20).orWhere(.name, "Bob").asDelete(),
        "DELETE FROM Person WHERE age = ? OR name = ?",
    );
}
