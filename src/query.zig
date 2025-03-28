const std = @import("std");
const util = @import("util.zig");
const Value = @import("value.zig").Value;
const Session = @import("session.zig").Session;
const RawQuery = @import("raw.zig").Query;
const Statement = @import("statement.zig").Statement;
const SqlBuf = @import("sql.zig").SqlBuf;

pub fn Query(comptime T: type) type {
    return struct {
        raw: RawQuery,

        const Q = @This();
        const Col = std.meta.FieldEnum(T);

        pub fn init(db: *Session) Q {
            return .{ .raw = RawQuery.init(db).select(util.columns(T)).table(util.tableName(T)) };
        }

        pub fn from(self: Q, sql: []const u8) Q {
            return .{ .raw = self.raw.from(sql) };
        }

        pub fn join(self: Q, sql: []const u8) Q {
            return .{ .raw = self.raw.join(sql) };
        }

        pub fn where(self: Q, comptime col: []const u8, val: util.ColType(T, col)) Q {
            return self.whereRaw(col ++ " = ?", val);
        }

        pub fn whereRaw(self: Q, sql: []const u8, args: anytype) Q {
            return .{ .raw = self.raw.where(sql, args) };
        }

        pub fn ifWhere(self: Q, cond: bool, comptime col: []const u8, val: util.ColType(T, col)) Q {
            return if (cond) self.where(col, val) else self;
        }

        pub fn maybeWhere(self: Q, comptime col: []const u8, val: util.MaybeColType(T, col)) Q {
            return if (val) |v| self.where(col, v) else self;
        }

        pub fn orWhere(self: Q, comptime col: []const u8, val: util.ColType(T, col)) Q {
            return self.orWhereRaw(col ++ " = ?", val);
        }

        pub fn orWhereRaw(self: Q, sql: []const u8, args: anytype) Q {
            return .{ .raw = self.raw.orWhere(sql, args) };
        }

        pub fn orIfWhere(self: Q, cond: bool, comptime col: []const u8, val: util.ColType(T, col)) Q {
            return if (cond) self.orWhere(col, val) else self;
        }

        pub fn orMaybeWhere(self: Q, comptime col: []const u8, val: util.MaybeColType(T, col)) Q {
            return if (val) |v| self.orWhere(col, v) else self;
        }

        pub fn orderBy(self: Q, col: Col, ord: enum { asc, desc }) Q {
            return self.orderByRaw(switch (col) {
                inline else => |c| switch (ord) {
                    inline else => |o| @tagName(c) ++ " " ++ @tagName(o),
                },
            });
        }

        pub fn orderByRaw(self: Q, sql: []const u8) Q {
            return .{ .raw = self.raw.orderBy(sql) };
        }

        pub fn limit(self: Q, n: i32) Q {
            return .{ .raw = self.raw.limit(n) };
        }

        pub fn offset(self: Q, i: i32) Q {
            return .{ .raw = self.raw.offset(i) };
        }

        pub fn get(self: Q, comptime col: []const u8) !?util.ColType(T, col) {
            return self.select(col).get(util.ColType(T, col));
        }

        pub fn exists(self: Q) !bool {
            return self.raw.exists();
        }

        pub fn count(self: Q, comptime col: []const u8) !u64 {
            return self.raw.count(col);
        }

        pub fn min(self: Q, comptime col: []const u8) !?util.ColType(T, col) {
            return self.select("MIN(" ++ col ++ ")").get(util.ColType(T, col));
        }

        pub fn max(self: Q, comptime col: []const u8) !?util.ColType(T, col) {
            return self.select("MAX(" ++ col ++ ")").get(util.ColType(T, col));
        }

        pub fn avg(self: Q, comptime col: []const u8) !?util.ColType(T, col) {
            return self.select("AVG(" ++ col ++ ")").get(util.ColType(T, col));
        }

        pub fn find(self: Q, id: util.Id(T)) !?T {
            return self.findBy("id", id);
        }

        pub fn findBy(self: Q, comptime col: []const u8, val: util.ColType(T, col)) !?T {
            return self.where(col, val).findFirst();
        }

        pub fn findFirst(self: Q) !?T {
            return self.limit(1).raw.fetchOne(T);
        }

        pub fn findAll(self: Q) ![]const T {
            return self.raw.fetchAll(T);
        }

        pub fn select(self: Q, sql: []const u8) RawQuery {
            return self.raw.select(sql);
        }

        pub fn pluck(self: Q, comptime col: []const u8) ![]const util.ColType(T, col) {
            return self.select(col).pluck(util.ColType(T, col));
        }

        pub fn groupBy(self: Q, sql: []const u8) RawQuery {
            return self.raw.groupBy(sql);
        }

        pub fn insert(self: Q, data: anytype) RawQuery {
            comptime util.checkFields(T, @TypeOf(data));

            return self.raw.insert().cols(comptime "(" ++ util.columns(@TypeOf(data)) ++ ")").values(comptime "(" ++ util.placeholders(@TypeOf(data)) ++ ")", data);
        }

        pub fn update(self: Q, data: anytype) RawQuery {
            comptime util.checkFields(T, @TypeOf(data));

            return self.raw.update().setAll(data);
        }

        pub fn delete(self: Q) RawQuery {
            return self.raw.delete();
        }

        pub fn toSql(self: Q, buf: *SqlBuf) !void {
            return buf.append(self.raw);
        }

        pub fn prepare(self: Q) !Statement {
            return self.raw.prepare();
        }
    };
}

const Person = struct {
    id: ?u32 = null,
    name: []const u8,
    age: u8,
};

const expectSql = @import("testing.zig").expectSql;
const fakeDb = @import("testing.zig").fakeDb;

test "query" {
    var db = try fakeDb();
    defer db.deinit();

    try expectSql(
        db.query(Person),
        "SELECT id, name, age FROM Person",
    );
}

test "query.select()" {
    var db = try fakeDb();
    defer db.deinit();

    try expectSql(
        db.query(Person).select("name"),
        "SELECT name FROM Person",
    );

    try expectSql(
        db.query(Person).select("name").select("age"),
        "SELECT age FROM Person",
    );

    try expectSql(
        db.query(Person).select("name, age"),
        "SELECT name, age FROM Person",
    );
}

test "query.join()" {
    var db = try fakeDb();
    defer db.deinit();

    try expectSql(
        db.query(Person).join("Address ON Person.id = Address.person_id"),
        "SELECT id, name, age FROM Person JOIN Address ON Person.id = Address.person_id",
    );
}

test "query.where()" {
    var db = try fakeDb();
    defer db.deinit();

    try expectSql(
        db.query(Person).where("name", "Alice"),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        db.query(Person).where("name", "Alice").where("age", 20),
        "SELECT id, name, age FROM Person WHERE name = ? AND age = ?",
    );

    try expectSql(
        db.query(Person).whereRaw("name = ?", "Alice").whereRaw("age > ?", 20),
        "SELECT id, name, age FROM Person WHERE name = ? AND age > ?",
    );

    try expectSql(
        db.query(Person).ifWhere(false, "name", "Alice"),
        "SELECT id, name, age FROM Person",
    );

    try expectSql(
        db.query(Person).ifWhere(true, "name", "Alice"),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        db.query(Person).maybeWhere("name", null),
        "SELECT id, name, age FROM Person",
    );

    try expectSql(
        // Check that arg type is "flat" optional even for opt columns
        db.query(Person).maybeWhere("id", @as(?u32, null)),
        "SELECT id, name, age FROM Person",
    );

    try expectSql(
        db.query(Person).maybeWhere("name", "Alice"),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );
}

test "query.orWhere()" {
    var db = try fakeDb();
    defer db.deinit();

    try expectSql(
        db.query(Person).orWhere("name", "Alice"),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        db.query(Person).where("name", "Alice").orWhere("age", 20),
        "SELECT id, name, age FROM Person WHERE name = ? OR age = ?",
    );

    try expectSql(
        db.query(Person).whereRaw("name = ?", "Alice").orWhereRaw("age > ?", 20),
        "SELECT id, name, age FROM Person WHERE name = ? OR age > ?",
    );

    try expectSql(
        db.query(Person).where("name", "Alice").orIfWhere(false, "age", 20),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        db.query(Person).where("name", "Alice").orIfWhere(true, "age", 20),
        "SELECT id, name, age FROM Person WHERE name = ? OR age = ?",
    );

    try expectSql(
        db.query(Person).where("name", "Alice").orMaybeWhere("age", null),
        "SELECT id, name, age FROM Person WHERE name = ?",
    );

    try expectSql(
        db.query(Person).where("name", "Alice").orMaybeWhere("age", 20),
        "SELECT id, name, age FROM Person WHERE name = ? OR age = ?",
    );
}

// TODO: select()
test "query.groupBy()" {
    var db = try fakeDb();
    defer db.deinit();

    try expectSql(
        db.query(Person).groupBy("name"),
        "SELECT id, name, age FROM Person GROUP BY name",
    );

    try expectSql(
        db.query(Person).groupBy("name").groupBy("age"),
        "SELECT id, name, age FROM Person GROUP BY name, age",
    );
}

test "query.orderBy()" {
    var db = try fakeDb();
    defer db.deinit();

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
    var db = try fakeDb();
    defer db.deinit();

    try expectSql(
        db.query(Person).limit(10),
        "SELECT id, name, age FROM Person LIMIT ?",
    );
}

test "query.offset()" {
    var db = try fakeDb();
    defer db.deinit();

    try expectSql(
        db.query(Person).offset(10),
        "SELECT id, name, age FROM Person OFFSET ?",
    );
}

test "query.insert()" {
    var db = try fakeDb();
    defer db.deinit();

    try expectSql(
        db.query(Person).insert(.{}),
        "INSERT INTO Person() VALUES ()",
    );

    try expectSql(
        db.query(Person).insert(.{ .name = "Alice", .age = 20 }),
        "INSERT INTO Person(name, age) VALUES (?, ?)",
    );

    try expectSql(
        db.query(Person).insert(.{ .name = "Alice", .age = 20 }).onConflict("DO NOTHING", {}),
        "INSERT INTO Person(name, age) VALUES (?, ?) ON CONFLICT DO NOTHING",
    );
}

test "query.update()" {
    var db = try fakeDb();
    defer db.deinit();

    try expectSql(
        db.query(Person).update(.{ .name = "Alice" }),
        "UPDATE Person SET name = ?",
    );

    try expectSql(
        db.query(Person).where("age", 20).update(.{ .name = "Alice" }),
        "UPDATE Person SET name = ? WHERE age = ?",
    );

    try expectSql(
        db.query(Person).where("age", 20).orWhere("name", "Bob").update(.{ .name = "Alice" }),
        "UPDATE Person SET name = ? WHERE age = ? OR name = ?",
    );

    try expectSql(
        db.query(Person).where("age", 20).update(.{ .name = "Alice", .age = 21 }),
        "UPDATE Person SET name = ?, age = ? WHERE age = ?",
    );

    try expectSql(
        db.query(Person).update(.{}).set("age = ?", 21),
        "UPDATE Person SET age = ?",
    );

    try expectSql(
        db.query(Person).update(.{ .name = "Alice" }).set("age = ?", 21).set("active = ?", true),
        "UPDATE Person SET name = ?, age = ?, active = ?",
    );
}

test "query.delete()" {
    var db = try fakeDb();
    defer db.deinit();

    try expectSql(
        db.query(Person).delete(),
        "DELETE FROM Person",
    );

    try expectSql(
        db.query(Person).where("age", 20).delete(),
        "DELETE FROM Person WHERE age = ?",
    );

    try expectSql(
        db.query(Person).where("age", 20).orWhere("name", "Bob").delete(),
        "DELETE FROM Person WHERE age = ? OR name = ?",
    );
}
