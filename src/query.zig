const std = @import("std");
const util = @import("util.zig");
const Value = @import("value.zig").Value;
const Session = @import("session.zig").Session;
const Statement = @import("statement.zig").Statement;

const RawPart = struct {
    prev: ?*const RawPart = null,
    sep: ?[]const u8 = null,
    sql: []const u8,
    args: []const Value = &.{},

    fn writeSql(self: *const RawPart, buf: *std.ArrayList(u8)) !void {
        if (self.prev) |prev| {
            try prev.writeSql(buf);

            if (self.sep) |s| {
                try buf.appendSlice(s);
            }
        }

        try buf.appendSlice(self.sql);
    }

    fn bind(self: *const RawPart, stmt: *Statement, i: *usize) !void {
        if (self.prev) |prev| {
            try prev.bind(stmt, i);
        }

        for (self.args) |v| {
            try stmt.bind(i.*, v);
            i.* = i.* + 1;
        }
    }
};

const State = struct {
    // IMPORTANT: Keep this in correct order. See `writeSql()` below.
    kind: enum { select, insert, update, delete } = .select,
    columns: ?*const RawPart = null,
    from: ?*const RawPart = null,
    data: ?*const RawPart = null,
    join: ?*const RawPart = null,
    where: ?*const RawPart = null,
    group_by: ?*const RawPart = null,
    order_by: ?*const RawPart = null,
    having: ?*const RawPart = null,
    limit: i32 = -1,
    offset: i32 = -1,
};

pub fn Query(comptime T: type, comptime R: type) type {
    return struct {
        session: *Session,
        state: State = .{},

        const Q = @This();
        const Col = std.meta.FieldEnum(T);

        /// Change the return type of the query.
        pub fn as(self: Q, comptime R2: type) Query(T, R2) {
            return @bitCast(self);
        }

        pub fn select(self: Q, comptime col: Col) Q {
            return self.selectRaw(@tagName(col));
        }

        pub fn selectRaw(self: Q, columns: []const u8) Q {
            return self.append(.columns, ", ", columns, .{});
        }

        pub fn where(self: Q, comptime col: Col, val: std.meta.FieldType(T, col)) Q {
            return self.whereRaw(@tagName(col) ++ " = ?", .{val});
        }

        pub fn whereRaw(self: Q, whr: []const u8, args: anytype) Q {
            return self.append(.where, " AND ", whr, args);
        }

        pub fn orWhere(self: Q, comptime col: Col, val: std.meta.FieldType(T, col)) Q {
            return self.orWhereRaw(@tagName(col) ++ " = ?", .{val});
        }

        pub fn orWhereRaw(self: Q, whr: []const u8, args: anytype) Q {
            return self.append(.where, " OR ", whr, args);
        }

        pub fn groupBy(self: Q, col: Col) Q {
            return self.groupByRaw(@tagName(col));
        }

        pub fn groupByRaw(self: Q, group_by: []const u8) Q {
            return self.append(.group_by, ", ", group_by, .{});
        }

        pub fn orderBy(self: Q, col: Col, ord: enum { asc, desc }) Q {
            return self.orderByRaw(switch (col) {
                inline else => |c| switch (ord) {
                    inline else => |o| @tagName(c) ++ " " ++ @tagName(o),
                },
            });
        }

        pub fn orderByRaw(self: Q, order_by: []const u8) Q {
            return self.append(.order_by, ", ", order_by, .{});
        }

        pub fn limit(self: Q, n: i32) Q {
            return self.replace(.limit, n);
        }

        pub fn offset(self: Q, i: i32) Q {
            return self.replace(.offset, i);
        }

        pub fn value(self: Q, comptime col: Col) !?std.meta.FieldType(T, col) {
            return self.valueRaw(std.meta.FieldType(T, col), @tagName(col));
        }

        pub fn valueRaw(self: Q, comptime V: type, expr: []const u8) !?V {
            var stmt = try self.select(expr).limit(1).prepare();
            defer stmt.deinit();

            return stmt.value(V);
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

        pub fn findFirst(self: Q) !?R {
            var stmt = try self.limit(1).prepare();
            defer stmt.deinit();

            return stmt.row(R);
        }

        pub fn findAll(self: Q) ![]const R {
            var stmt = try self.prepare();
            defer stmt.deinit();

            return stmt.all(R);
        }

        pub fn insert(self: Q, data: anytype) Q {
            return self.replace(.kind, .insert).values(data);
        }

        pub fn values(self: Q, data: anytype) Q {
            comptime util.checkFields(T, @TypeOf(data));

            return self.replace(.data, self.raw(null, comptime "(" ++ util.columns(@TypeOf(data)) ++ ") VALUES (" ++ util.placeholders(@TypeOf(data)) ++ ")", data));
        }

        pub fn update(self: Q, data: anytype) Q {
            return self.replace(.kind, .update).setAll(data);
        }

        // pub fn set(self: Q, comptime col: Col, val: std.meta.FieldType(T, col)) Q {}
        // pub fn setRaw(self: Q, comptime col: Col, sql: []const u8) Q {}

        pub fn setAll(self: Q, data: anytype) Q {
            comptime util.checkFields(T, @TypeOf(data));

            return self.replace(.data, self.raw(null, comptime " SET " ++ util.setters(@TypeOf(data)), data));
        }

        pub fn delete(self: Q) Q {
            return self.replace(.kind, .delete);
        }

        pub fn prepare(self: Q) !Statement {
            var buf = std.ArrayList(u8).init(self.session.arena);
            defer buf.deinit();

            try self.writeSql(&buf);

            var stmt = try self.session.prepare(buf.items, .{});
            errdefer stmt.deinit();

            var i: usize = 0;
            inline for (@typeInfo(State).Struct.fields) |f| {
                switch (f.type) {
                    ?*const RawPart => if (@field(self.state, f.name)) |part| {
                        try part.bind(&stmt, &i);
                    },

                    i32 => if (@field(self.state, f.name) >= 0) {
                        try stmt.bind(i, @field(self.state, f.name));
                        i += 1;
                    },

                    else => {},
                }
            }

            return stmt;
        }

        pub fn writeSql(self: Q, buf: *std.ArrayList(u8)) !void {
            try buf.appendSlice(switch (self.state.kind) {
                .select => "SELECT ",
                .insert => "INSERT INTO ",
                .update => "UPDATE ",
                .delete => "DELETE FROM ",
            });

            if (self.state.kind == .select) {
                if (self.state.columns) |part| {
                    try part.writeSql(buf);
                } else {
                    try buf.appendSlice(util.columns(R));
                }

                try buf.appendSlice(" FROM ");
            }

            try buf.appendSlice(util.tableName(T));

            if (self.state.data) |part| {
                try part.writeSql(buf);
            }

            if (self.state.join) |part| {
                try buf.appendSlice(" JOIN ");
                try part.writeSql(buf);
            }

            if (self.state.where) |part| {
                try buf.appendSlice(" WHERE ");
                try part.writeSql(buf);
            }

            if (self.state.group_by) |part| {
                try buf.appendSlice(" GROUP BY ");
                try part.writeSql(buf);
            }

            if (self.state.order_by) |part| {
                try buf.appendSlice(" ORDER BY ");
                try part.writeSql(buf);
            }

            if (self.state.having) |part| {
                try buf.appendSlice(" HAVING ");
                try part.writeSql(buf);
            }

            if (self.state.limit >= 0) {
                try buf.appendSlice(" LIMIT ?");
            }

            if (self.state.offset >= 0) {
                try buf.appendSlice(" OFFSET ?");
            }
        }

        fn replace(self: Q, comptime field: std.meta.FieldEnum(State), val: std.meta.FieldType(State, field)) Q {
            var copy = self;
            @field(copy.state, @tagName(field)) = val;
            return copy;
        }

        fn append(self: Q, comptime field: std.meta.FieldEnum(State), sep: ?[]const u8, sql: []const u8, args: anytype) Q {
            const part = self.raw(sep, sql, args);
            part.prev = @field(self.state, @tagName(field));

            return self.replace(field, part);
        }

        fn raw(self: Q, sep: ?[]const u8, sql: []const u8, args: anytype) *RawPart {
            const res = self.session.arena.create(RawPart) catch @panic("OOM");
            res.* = .{
                .sep = sep,
                .sql = sql,
            };

            const fields = @typeInfo(@TypeOf(args)).Struct.fields;
            if (comptime fields.len > 0) {
                const vals = self.session.arena.alloc(Value, fields.len) catch @panic("OOM");

                inline for (fields, 0..) |f, i| {
                    vals[i] = Value.from(@field(args, f.name), self.session.arena) catch @panic("OOM");
                }

                res.args = vals;
            }

            return res;
        }
    };
}

const Person = struct {
    id: u32,
    name: []const u8,
    age: u8,
};

var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
var db: Session = .{ .arena = arena.allocator(), .conn = undefined };

fn expectSql(q: Query(Person, Person), sql: []const u8) !void {
    defer _ = arena.reset(.free_all);

    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    try q.writeSql(&buf);
    try std.testing.expectEqualStrings(sql, buf.items);
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
        db.query(Person).insert(.{}),
        "INSERT INTO Person() VALUES ()",
    );

    try expectSql(
        db.query(Person).insert(.{ .name = "Alice", .age = 20 }),
        "INSERT INTO Person(name, age) VALUES (?, ?)",
    );
}

test "query.update()" {
    try expectSql(
        db.query(Person).update(.{ .name = "Alice" }),
        "UPDATE Person SET name = ?",
    );

    try expectSql(
        db.query(Person).where(.age, 20).update(.{ .name = "Alice" }),
        "UPDATE Person SET name = ? WHERE age = ?",
    );

    try expectSql(
        db.query(Person).where(.age, 20).orWhere(.name, "Bob").update(.{ .name = "Alice" }),
        "UPDATE Person SET name = ? WHERE age = ? OR name = ?",
    );

    try expectSql(
        db.query(Person).where(.age, 20).update(.{ .name = "Alice", .age = 21 }),
        "UPDATE Person SET name = ?, age = ? WHERE age = ?",
    );
}

test "query.delete()" {
    try expectSql(
        db.query(Person).delete(),
        "DELETE FROM Person",
    );

    try expectSql(
        db.query(Person).where(.age, 20).delete(),
        "DELETE FROM Person WHERE age = ?",
    );

    try expectSql(
        db.query(Person).where(.age, 20).orWhere(.name, "Bob").delete(),
        "DELETE FROM Person WHERE age = ? OR name = ?",
    );
}
