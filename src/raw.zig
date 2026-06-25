const std = @import("std");
const Session = @import("session.zig").Session;
const Value = @import("value.zig").Value;
const Statement = @import("statement.zig").Statement;
const SqlBuf = @import("sql.zig").SqlBuf;
const util = @import("util.zig");

pub const Part = struct {
    kind: Kind,
    sql: []const u8 = "",
    args: Args = .none,

    pub const Kind = enum { raw, SELECT, INSERT, UPDATE, DELETE, table, cols, VALUES, SET, JOIN, @"LEFT JOIN", WHERE, AND, OR, @"GROUP BY", HAVING, @"ORDER BY", LIMIT, OFFSET, @"ON CONFLICT", RETURNING };
};

pub const RawQuery = struct {
    db: *Session,
    begin: usize,
    mask: u64 = 0,

    pub fn init(db: *Session) RawQuery {
        return .{ .db = db, .begin = db.parts.items.len };
    }

    pub fn raw(db: *Session, sql: []const u8, args: anytype) RawQuery {
        return init(db).append(.raw, sql, .from(args, db));
    }

    pub fn table(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.table, sql, .none);
    }

    pub fn insert(self: RawQuery) RawQuery {
        return self.append(.INSERT, "", .none);
    }

    pub const into = table;

    pub fn cols(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.cols, sql, .none);
    }

    pub fn values(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        return self.append(.VALUES, sql, .fromFields(args, self.db));
    }

    pub fn onConflict(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        return self.append(.@"ON CONFLICT", sql, .from(args, self.db));
    }

    pub fn update(self: RawQuery) RawQuery {
        return self.append(.UPDATE, "", .none);
    }

    pub fn set(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        return self.append(.SET, sql, .from(args, self.db));
    }

    pub fn setAll(self: RawQuery, data: anytype) RawQuery {
        if (comptime std.meta.fieldNames(@TypeOf(data)).len == 0) {
            return self;
        }

        return self.append(.SET, util.setters(@TypeOf(data)), .fromFields(data, self.db));
    }

    pub fn delete(self: RawQuery) RawQuery {
        return self.append(.DELETE, "", .none);
    }

    pub fn select(self: RawQuery, sql: []const u8) RawQuery {
        return self.selectRaw(sql, {});
    }

    pub fn selectRaw(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        return self.append(.SELECT, sql, .from(args, self.db));
    }

    pub const from = table;

    pub fn join(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.JOIN, sql, .none);
    }

    pub fn leftJoin(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.@"LEFT JOIN", sql, .none);
    }

    pub fn where(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        // TODO: emit WHERE/AND in toSql()
        return self.append(.WHERE, sql, .from(args, self.db));
    }

    pub fn ifWhere(self: RawQuery, cond: bool, sql: []const u8, args: anytype) RawQuery {
        return if (cond) self.where(sql, args) else self;
    }

    pub fn maybeWhere(self: RawQuery, comptime sql: []const u8, arg: anytype) RawQuery {
        return if (arg) |v| self.where(sql, v) else self;
    }

    pub fn orWhere(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        // TODO: emit WHERE/OR in toSql()
        return self.append(.OR, sql, .from(args, self.db));
    }

    pub fn orIfWhere(self: RawQuery, cond: bool, sql: []const u8, args: anytype) RawQuery {
        return if (cond) self.orWhere(sql, args) else self;
    }

    pub fn orMaybeWhere(self: RawQuery, comptime sql: []const u8, arg: anytype) RawQuery {
        return if (arg) |v| self.orWhere(sql, v) else self;
    }

    pub fn groupBy(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.@"GROUP BY", sql, .none);
    }

    pub fn having(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        return self.append(.HAVING, sql, .from(args, self.db));
    }

    pub fn orderBy(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.@"ORDER BY", sql, .none);
    }

    pub fn limit(self: RawQuery, n: i32) RawQuery {
        return self.append(.LIMIT, "?", .{ .one = .{ .int = @intCast(n) } });
    }

    pub fn offset(self: RawQuery, i: i32) RawQuery {
        return self.append(.OFFSET, "?", .{ .one = .{ .int = @intCast(i) } });
    }

    pub fn returning(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.RETURNING, sql, .none);
    }

    pub fn exec(self: RawQuery) !void {
        var stmt = try self.prepare();
        defer stmt.deinit();

        try stmt.exec();
    }

    pub fn get(self: RawQuery, comptime T: type) !?T {
        var stmt = try self.prepare();
        defer stmt.deinit();

        return if (try stmt.next(?T, self.db.arena)) |v| v else null;
    }

    pub fn exists(self: RawQuery) !bool {
        return try self.select("1").get(bool) orelse false;
    }

    pub fn count(self: RawQuery, comptime col: []const u8) !u64 {
        return (try self.select("COUNT(" ++ col ++ ")").get(u64)).?;
    }

    pub fn pluck(self: RawQuery, comptime R: type) ![]const R {
        var stmt = try self.prepare();
        defer stmt.deinit();

        var res = std.array_list.Managed(R).init(self.db.arena);
        errdefer res.deinit();

        while (try stmt.next(struct { R }, self.db.arena)) |row| {
            try res.append(row[0]);
        }

        return res.toOwnedSlice();
    }

    pub fn fetchOne(self: RawQuery, comptime R: type) !?R {
        var stmt = try self.prepare();
        defer stmt.deinit();

        return stmt.next(R, self.db.arena);
    }

    pub fn fetchAll(self: RawQuery, comptime R: type) ![]const R {
        var stmt = try self.prepare();
        defer stmt.deinit();

        var res = std.array_list.Managed(R).init(self.db.arena);
        errdefer res.deinit();

        while (try stmt.next(R, self.db.arena)) |row| {
            try res.append(row);
        }

        return res.toOwnedSlice();
    }

    pub fn prepare(self: RawQuery) !Statement {
        // Collect indices from the mask
        var ibuf: [64]*Part = undefined;
        const parts = ibuf[0..@popCount(self.mask)];
        var mask = self.mask;
        for (parts) |*part| {
            const bit: u6 = @intCast(@ctz(mask));
            part.* = &self.db.parts.items[self.begin..][bit];
            mask &= ~(@as(u64, 1) << bit);
        }

        // Sort
        const H = struct {
            fn lt(_: void, a: *Part, b: *Part) bool {
                return @intFromEnum(a.kind) < @intFromEnum(b.kind);
            }
        };
        std.mem.sort(*Part, parts, {}, H.lt);

        // Render in sorted order, tracking prev kind for proper separators
        var buf = try SqlBuf.init(self.db.arena);
        var prev_kind: ?Part.Kind = null;
        for (parts) |part| {
            if (part.kind != .raw) {
                const comma = switch (part.kind) {
                    .SELECT, .@"GROUP BY", .@"ORDER BY", .SET => prev_kind != null and prev_kind.? == part.kind,
                    else => false,
                };

                try buf.append(if (comma) ", " else switch (part.kind) {
                    .SELECT => "SELECT ",
                    inline .INSERT, .UPDATE, .DELETE => |t| @tagName(t),
                    .cols => "",
                    .table => switch (std.ascii.toLower(buf.buf.items[1])) {
                        'e' => " FROM ", // SeLECT, DeLETE
                        'n' => " INTO ", // InSERT
                        else => " ",
                    },
                    .WHERE => if (prev_kind == .WHERE or prev_kind == .OR) " AND " else " WHERE ",
                    .OR => if (prev_kind == .WHERE or prev_kind == .OR) " OR " else " WHERE ",
                    inline else => |t| " " ++ @tagName(t) ++ " ",
                });
            }

            try buf.append(part.sql);
            prev_kind = part.kind;
        }

        var stmt: Statement = try self.db.conn.prepare(buf.buf.items);
        errdefer stmt.deinit();

        // Bind args
        var i: usize = 0;
        for (parts) |part| try part.args.bind(&stmt, &i);

        return stmt;
    }

    pub fn append(self: RawQuery, kind: Part.Kind, sql: []const u8, args: Args) RawQuery {
        const bit = self.db.parts.items.len - self.begin;
        self.db.parts.append(self.db.arena, .{ .kind = kind, .sql = sql, .args = args }) catch @panic("OOM");
        var copy = self;
        copy.mask |= @as(u64, 1) << @intCast(bit);
        return copy;
    }
};

// TODO: we should either add Session.args and store [begin, end] in the Part,
//       or we could make Part tagged-union and arg could be one of the variants
//       in the same list (+add Part.n_args). Either way is probably better than this.
const Args = union(enum) {
    none,
    one: Value,
    many: []const Value,

    fn from(args: anytype, db: *Session) Args {
        if (comptime @TypeOf(args) == Args) return args;
        if (comptime @TypeOf(args) == void) return .none;
        if (comptime util.isTuple(@TypeOf(args))) return fromFields(args, db);

        return .{ .one = Value.from(args, db.arena) catch @panic("OOM") };
    }

    fn fromFields(args: anytype, db: *Session) Args {
        const fields = @typeInfo(@TypeOf(args)).@"struct".field_names;
        const res = db.arena.alloc(Value, fields.len) catch @panic("OOM");
        inline for (fields, 0..) |f, i| res[i] = Value.from(@field(args, f), db.arena) catch @panic("OOM");

        return .{ .many = res };
    }

    fn bind(self: Args, stmt: anytype, i: *usize) !void {
        switch (self) {
            .none => {},
            .one => |arg| {
                try stmt.bind(i.*, arg);
                i.* += 1;
            },
            .many => |args| for (args) |arg| {
                try stmt.bind(i.*, arg);
                i.* += 1;
            },
        }
    }
};

const expectSql = @import("testing.zig").expectSql;
const fakeDb = @import("testing.zig").fakeDb;

test "select" {
    var db = try fakeDb();
    defer db.deinit();
    const select1 = RawQuery.init(&db).select("1");

    try expectSql(select1, "SELECT 1");
    // try expectSql(select1.select("2"), "SELECT 1, 2");
    // try expectSql(select1.selectRaw("?", 1), "SELECT 1, ?");
}

test "insert" {
    var db = try fakeDb();
    defer db.deinit();
    const insert = RawQuery.init(&db).insert();

    try expectSql(insert, "INSERT");
    try expectSql(insert.into("Person"), "INSERT INTO Person");
    try expectSql(insert.into("Person").returning("id"), "INSERT INTO Person RETURNING id");
    try expectSql(insert.into("Person").cols("(name, age)"), "INSERT INTO Person(name, age)");
    try expectSql(insert.into("Person").cols("(name, age)").values("(?, ?)", .{ "Alice", 18 }), "INSERT INTO Person(name, age) VALUES (?, ?)");
}

test "update" {
    var db = try fakeDb();
    defer db.deinit();
    const update = RawQuery.init(&db).update();

    try expectSql(update, "UPDATE");
    try expectSql(update.table("Person"), "UPDATE Person");
    try expectSql(update.table("Person").set("age = ?", 18), "UPDATE Person SET age = ?");
}

test "delete" {
    var db = try fakeDb();
    defer db.deinit();
    const delete = RawQuery.init(&db).delete();

    try expectSql(delete, "DELETE");
    try expectSql(delete.from("Person"), "DELETE FROM Person");
    try expectSql(delete.from("Person").where("age < ?", 18), "DELETE FROM Person WHERE age < ?");
}

test "raw" {
    var db = try fakeDb();
    defer db.deinit();
    const raw = db.raw("SELECT DISTINCT name", {});

    try expectSql(raw, "SELECT DISTINCT name");
    try expectSql(raw.from("Person"), "SELECT DISTINCT name FROM Person");
    try expectSql(raw.from("Person").where("age > ?", 18), "SELECT DISTINCT name FROM Person WHERE age > ?");
}
