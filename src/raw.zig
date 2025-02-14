const std = @import("std");
const Session = @import("session.zig").Session;
const Value = @import("value.zig").Value;
const Statement = @import("statement.zig").Statement;
const SqlBuf = @import("sql.zig").SqlBuf;
const util = @import("util.zig");

const Part = struct {
    prev: ?*const Part = null,
    kind: Kind,
    sql: []const u8 = "",
    args: Args = .empty,

    pub const Kind = enum { raw, cols, table, SELECT, INSERT, UPDATE, DELETE, JOIN, @"LEFT JOIN", WHERE, AND, OR, @"GROUP BY", HAVING, @"ORDER BY", VALUES, SET, @"ON CONFLICT", RETURNING };

    pub fn toSql(self: Part, buf: *SqlBuf) !void {
        if (self.prev) |p| try buf.append(p);

        if (self.kind != .raw and self.kind != .cols and self.kind != .table) {
            const comma = switch (self.kind) {
                .SELECT, .@"GROUP BY", .@"ORDER BY", .SET => self.prev != null and self.prev.?.kind == self.kind,
                else => false,
            };

            try buf.append(if (comma) ", " else switch (self.kind) {
                .SELECT => "SELECT ",
                inline .INSERT, .UPDATE, .DELETE => |t| @tagName(t),
                inline else => |t| " " ++ @tagName(t) ++ " ",
            });
        }

        try buf.append(self.sql);
    }

    fn bind(self: Part, stmt: anytype, i: *usize) !void {
        if (self.prev) |p| try p.bind(stmt, i);

        try self.args.bind(stmt, i);
    }
};

pub const Query = struct {
    db: *Session,
    parts: struct {
        head: ?*const Part = null, //   <raw>, SELECT, INSERT, UPDATE, DELETE
        tables: ?*const Part = null, // JOIN, LEFT JOIN, cols, VALUES, SET
        where: ?*const Part = null, //  WHERE, AND, OR
        tail: ?*const Part = null, //   everything else
    } = .{},

    pub fn init(db: *Session) Query {
        return .{ .db = db };
    }

    pub fn raw(db: *Session, sql: []const u8, args: anytype) Query {
        return init(db).append(.raw, sql, args);
    }

    pub fn table(self: Query, sql: []const u8) Query {
        const part = self.db.arena.create(Part) catch @panic("OOM");
        part.* = .{ .kind = .table, .sql = sql };
        return self.replace(part);
    }

    pub fn insert(self: Query) Query {
        return self.replace(comptime &.{ .kind = .INSERT });
    }

    pub const into = table;

    pub fn cols(self: Query, sql: []const u8) Query {
        return self.append(.cols, sql, .{});
    }

    pub fn values(self: Query, sql: []const u8, args: anytype) Query {
        return self.append(.VALUES, sql, args);
    }

    pub fn onConflict(self: Query, sql: []const u8, args: anytype) Query {
        return self.append(.@"ON CONFLICT", sql, args);
    }

    pub fn update(self: Query) Query {
        return self.replace(comptime &.{ .kind = .UPDATE });
    }

    pub fn set(self: Query, sql: []const u8, args: anytype) Query {
        return self.append(.SET, sql, args);
    }

    pub fn setAll(self: Query, data: anytype) Query {
        if (comptime std.meta.fields(@TypeOf(data)).len == 0) {
            return self;
        }

        return self.append(.SET, util.setters(@TypeOf(data)), data);
    }

    pub fn delete(self: Query) Query {
        return self.replace(comptime &.{ .kind = .DELETE });
    }

    pub fn select(self: Query, sql: []const u8) Query {
        const part = self.db.arena.create(Part) catch @panic("OOM");
        part.* = .{ .kind = .SELECT, .sql = sql };
        return self.replace(part);
    }

    pub const from = table;

    pub fn join(self: Query, sql: []const u8) Query {
        return self.append(.JOIN, sql, .{});
    }

    pub fn leftJoin(self: Query, sql: []const u8) Query {
        return self.append(.@"LEFT JOIN", sql, .{});
    }

    pub fn where(self: Query, sql: []const u8, args: anytype) Query {
        return self.append(if (self.parts.where == null) .WHERE else .AND, sql, args);
    }

    pub fn orWhere(self: Query, sql: []const u8, args: anytype) Query {
        return self.append(if (self.parts.where == null) .WHERE else .OR, sql, args);
    }

    pub fn groupBy(self: Query, sql: []const u8) Query {
        return self.append(.@"GROUP BY", sql, .{});
    }

    pub fn having(self: Query, sql: []const u8, args: anytype) Query {
        return self.append(.HAVING, sql, args);
    }

    pub fn orderBy(self: Query, sql: []const u8) Query {
        return self.append(.@"ORDER BY", sql, .{});
    }

    pub fn limit(self: Query, n: i32) Query {
        return self.append(.raw, " LIMIT ?", .{n});
    }

    pub fn offset(self: Query, i: i32) Query {
        return self.append(.raw, " OFFSET ?", .{i});
    }

    pub fn returning(self: Query, sql: []const u8) Query {
        return self.append(.RETURNING, sql, .{});
    }

    pub fn exec(self: Query) !void {
        var stmt = try self.prepare();
        defer stmt.deinit();

        try stmt.exec();
    }

    pub fn get(self: Query, comptime T: type) !?T {
        var stmt = try self.prepare();
        defer stmt.deinit();

        if (try stmt.next(struct { T }, self.db.arena)) |row| {
            return row[0];
        }

        return null;
    }

    pub fn exists(self: Query) !bool {
        return try self.select("1").get(bool) orelse false;
    }

    pub fn count(self: Query, comptime col: []const u8) !u64 {
        return (try self.select("COUNT(" ++ col ++ ")").get(u64)).?;
    }

    pub fn pluck(self: Query, comptime R: type) ![]const R {
        var stmt = try self.prepare();
        defer stmt.deinit();

        var res = std.ArrayList(R).init(self.db.arena);
        errdefer res.deinit();

        while (try stmt.next(struct { R }, self.db.arena)) |row| {
            try res.append(row[0]);
        }

        return res.toOwnedSlice();
    }

    pub fn fetchOne(self: Query, comptime R: type) !?R {
        var stmt = try self.prepare();
        defer stmt.deinit();

        return stmt.next(R, self.db.arena);
    }

    pub fn fetchAll(self: Query, comptime R: type) ![]const R {
        var stmt = try self.prepare();
        defer stmt.deinit();

        var res = std.ArrayList(R).init(self.db.arena);
        errdefer res.deinit();

        while (try stmt.next(R, self.db.arena)) |row| {
            try res.append(row);
        }

        return res.toOwnedSlice();
    }

    pub fn toSql(self: Query, buf: *SqlBuf) !void {
        if (self.parts.head) |h| try buf.append(h);

        if (self.parts.tables) |t| {
            std.debug.assert(buf.buf.items.len > 1);
            try buf.append(switch (std.ascii.toLower(buf.buf.items[1])) {
                'e' => " FROM ", // SeLECT, DeLETE
                'n' => " INTO ", // InSERT
                else => " ",
            });

            try buf.append(t);
        }

        if (self.parts.where) |w| try buf.append(w);
        if (self.parts.tail) |t| try buf.append(t);
    }

    pub fn prepare(self: Query) !Statement {
        var buf = try SqlBuf.init(self.db.arena);
        try buf.append(self);

        var stmt = try self.db.prepare(buf.buf.items, .{});
        errdefer stmt.deinit();

        var i: usize = 0;
        if (self.parts.head) |p| try p.bind(&stmt, &i);
        if (self.parts.tables) |t| try t.bind(&stmt, &i);
        if (self.parts.where) |w| try w.bind(&stmt, &i);
        if (self.parts.tail) |t| try t.bind(&stmt, &i);

        return stmt;
    }

    pub fn append(self: Query, kind: Part.Kind, sql: []const u8, args: anytype) Query {
        const part = self.db.arena.create(Part) catch @panic("OOM");
        part.* = .{ .prev = self.slot(kind).*, .kind = kind, .sql = sql, .args = toArgs(self.db, args) };
        return self.replace(part);
    }

    fn replace(self: Query, part: *const Part) Query {
        var copy = self;
        copy.slot(part.kind).* = part;
        return copy;
    }

    fn slot(self: anytype, kind: Part.Kind) switch (@TypeOf(self)) {
        *const Query => *const ?*const Part,
        *Query => *?*const Part,
        else => unreachable,
    } {
        return switch (kind) {
            .raw => if (self.parts.head == null) &self.parts.head else &self.parts.tail,
            .SELECT, .INSERT, .UPDATE, .DELETE => &self.parts.head,
            .table, .cols, .VALUES, .SET, .JOIN, .@"LEFT JOIN" => &self.parts.tables,
            .WHERE, .AND, .OR => &self.parts.where,
            else => &self.parts.tail,
        };
    }

    fn toArgs(db: *Session, args: anytype) Args {
        const fields = @typeInfo(@TypeOf(args)).@"struct".fields;

        return switch (fields.len) {
            0 => .empty,
            1 => .{ .one = Value.from(@field(args, fields[0].name), db.arena) catch @panic("OOM") },
            else => {
                const res = db.arena.alloc(Value, fields.len) catch @panic("OOM");

                inline for (fields, 0..) |f, i| {
                    res[i] = Value.from(@field(args, f.name), db.arena) catch @panic("OOM");
                }

                return .{ .many = res };
            },
        };
    }
};

const Args = union(enum) {
    empty,
    one: Value,
    many: []const Value,

    fn bind(self: Args, stmt: anytype, i: *usize) !void {
        switch (self) {
            .empty => {},
            .one => |arg| try arg.bind(stmt, i),
            .many => |args| for (args) |arg| try arg.bind(stmt, i),
        }
    }
};

const expectSql = @import("testing.zig").expectSql;
const fakeDb = @import("testing.zig").fakeDb;

test "select" {
    var db = try fakeDb();
    defer db.deinit();
    const select1 = Query.init(&db).select("1");

    try expectSql(select1, "SELECT 1");
    try expectSql(select1.select("2"), "SELECT 2");
}

test "insert" {
    var db = try fakeDb();
    defer db.deinit();
    const insert = Query.init(&db).insert();

    try expectSql(insert, "INSERT");
    try expectSql(insert.into("Person"), "INSERT INTO Person");
    try expectSql(insert.into("Person").returning("id"), "INSERT INTO Person RETURNING id");
    try expectSql(insert.into("Person").cols("(name, age)"), "INSERT INTO Person(name, age)");
    try expectSql(insert.into("Person").cols("(name, age)").values("(?, ?)", .{ "Alice", 18 }), "INSERT INTO Person(name, age) VALUES (?, ?)");
}

test "update" {
    var db = try fakeDb();
    defer db.deinit();
    const update = Query.init(&db).update();

    try expectSql(update, "UPDATE");
    try expectSql(update.table("Person"), "UPDATE Person");
    try expectSql(update.table("Person").set("age = ?", .{18}), "UPDATE Person SET age = ?");
}

test "delete" {
    var db = try fakeDb();
    defer db.deinit();
    const delete = Query.init(&db).delete();

    try expectSql(delete, "DELETE");
    try expectSql(delete.from("Person"), "DELETE FROM Person");
    try expectSql(delete.from("Person").where("age < ?", .{18}), "DELETE FROM Person WHERE age < ?");
}

test "raw" {
    var db = try fakeDb();
    defer db.deinit();
    const raw = db.raw("SELECT DISTINCT name", .{});

    try expectSql(raw, "SELECT DISTINCT name");
    try expectSql(raw.from("Person"), "SELECT DISTINCT name FROM Person");
    try expectSql(raw.from("Person").where("age > ?", .{18}), "SELECT DISTINCT name FROM Person WHERE age > ?");
}
