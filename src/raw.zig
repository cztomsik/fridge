const std = @import("std");
const Session = @import("session.zig").Session;
const Value = @import("value.zig").Value;
const Statement = @import("statement.zig").Statement;
const util = @import("util.zig");

// Few notes:
// - queries are arena-scoped, so an ArrayList(T) is not a good idea (resize)
// - linked lists were also awkward (and a single list with re-order might be even worse)
// - so this (session-global list + mask) is a good trade-off IMO
// - a bounded array could work too, but mask is simpler (builder methods are cheap)
// - immutability was not a hard constraint, but it comes for free with this design
// - I am still not 100% sure if re-ordering is worth it, but it's a single pre-render step now
// - (consecutive) builder methods need to be called in the (sliding) window of 64 parts
//   - we could probably check last_offset and do a simple copy-on-write (maybe later, IDK)
// - we currently have 2 separate lists for parts & values; we could probably encode both in
//   one, but I'm not sure if it's worth it. maybe if Part becomes a tagged union, then this
//   would make more sense.
// - this was "invented" on paper — it helps a lot to draw this out

pub const Part = struct {
    kind: Kind,
    sql: []const u8 = "",
    args: [2]usize,

    pub const Kind = enum { raw, ident, SELECT, table, cols, VALUES, SET, JOIN, @"LEFT JOIN", WHERE, OR, @"GROUP BY", HAVING, @"ORDER BY", LIMIT, OFFSET, @"ON CONFLICT", RETURNING };

    pub fn order(self: Part) u8 {
        return switch (self.kind) {
            .raw, .ident => unreachable,
            .JOIN, .@"LEFT JOIN" => @intFromEnum(Kind.JOIN),
            .WHERE, .OR => @intFromEnum(Kind.WHERE),
            else => |k| @intFromEnum(k),
        };
    }
};

pub const RawQuery = struct {
    db: *Session,
    begin: u32,
    prefix: Prefix = .none,
    mask: u64 = 0,

    const Prefix = enum { none, select, @"INSERT INTO", UPDATE, @"DELETE FROM" };

    comptime {
        std.debug.assert(@sizeOf(RawQuery) == 24);
    }

    pub fn init(db: *Session) RawQuery {
        return .{ .db = db, .begin = @intCast(db.parts.items.len) };
    }

    pub fn withPrefix(self: RawQuery, prefix: Prefix) RawQuery {
        var copy = self;
        copy.prefix = prefix;
        return copy;
    }

    pub fn apply(self: RawQuery, x: anytype) RawQuery {
        return x.build(self);
    }

    pub fn appendRaw(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        return self.append(.raw, sql, args);
    }

    pub fn appendIdent(self: RawQuery, name: []const u8) RawQuery {
        return self.append(.ident, name, {});
    }

    pub fn table(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.table, sql, {});
    }

    pub fn insert(self: RawQuery) RawQuery {
        var copy = self;
        copy.prefix = .@"INSERT INTO";
        return copy;
    }

    pub const into = table;

    pub fn cols(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.cols, sql, {});
    }

    pub fn values(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        return self.append(.VALUES, sql, args);
    }

    pub fn onConflict(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        return self.append(.@"ON CONFLICT", sql, args);
    }

    // TODO: maybe set() could also call withPrefix(.update)?
    pub fn update(self: RawQuery) RawQuery {
        return self.withPrefix(.UPDATE);
    }

    pub fn set(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        return self.append(.SET, sql, args);
    }

    pub fn setAll(self: RawQuery, data: anytype) RawQuery {
        if (comptime std.meta.fieldNames(@TypeOf(data)).len == 0) {
            return self;
        }

        return self.set(util.setters(@TypeOf(data)), data);
    }

    pub fn delete(self: RawQuery) RawQuery {
        return self.withPrefix(.@"DELETE FROM");
    }

    pub fn select(self: RawQuery, sql: []const u8) RawQuery {
        return self.selectRaw(sql, {});
    }

    pub fn selectRaw(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        return self.withPrefix(.none).append(.SELECT, sql, args);
    }

    pub const from = table;

    pub fn join(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.JOIN, sql, {});
    }

    pub fn leftJoin(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.@"LEFT JOIN", sql, {});
    }

    pub fn where(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        return self.append(.WHERE, sql, args);
    }

    pub fn ifWhere(self: RawQuery, cond: bool, sql: []const u8, args: anytype) RawQuery {
        return if (cond) self.where(sql, args) else self;
    }

    pub fn maybeWhere(self: RawQuery, comptime sql: []const u8, arg: anytype) RawQuery {
        return if (arg) |v| self.where(sql, v) else self;
    }

    pub fn orWhere(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        return self.append(.OR, sql, args);
    }

    pub fn orIfWhere(self: RawQuery, cond: bool, sql: []const u8, args: anytype) RawQuery {
        return if (cond) self.orWhere(sql, args) else self;
    }

    pub fn orMaybeWhere(self: RawQuery, comptime sql: []const u8, arg: anytype) RawQuery {
        return if (arg) |v| self.orWhere(sql, v) else self;
    }

    pub fn groupBy(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.@"GROUP BY", sql, {});
    }

    pub fn having(self: RawQuery, sql: []const u8, args: anytype) RawQuery {
        return self.append(.HAVING, sql, args);
    }

    pub fn orderBy(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.@"ORDER BY", sql, {});
    }

    pub fn limit(self: RawQuery, n: i32) RawQuery {
        return self.append(.LIMIT, "?", n);
    }

    pub fn offset(self: RawQuery, i: i32) RawQuery {
        return self.append(.OFFSET, "?", i);
    }

    pub fn returning(self: RawQuery, sql: []const u8) RawQuery {
        return self.append(.RETURNING, sql, {});
    }

    pub fn exec(self: RawQuery) !void {
        var stmt = try self.prepare(undefined);
        defer stmt.deinit();

        try stmt.exec();
    }

    pub fn get(self: RawQuery, comptime T: type) !?T {
        var stmt = try self.prepare(undefined);
        defer stmt.deinit();

        if (!try stmt.step()) return null;
        return try (try stmt.column(0)).into(?T, self.db.arena);
    }

    pub fn exists(self: RawQuery) !bool {
        return try self.select("1").get(bool) orelse false;
    }

    pub fn count(self: RawQuery, comptime col: []const u8) !u64 {
        return (try self.select("COUNT(" ++ col ++ ")").get(u64)).?;
    }

    pub fn pluck(self: RawQuery, comptime T: type) ![]const T {
        var stmt = try self.prepare(undefined);
        defer stmt.deinit();

        var res: std.ArrayList(T) = .empty;
        while (try stmt.step()) {
            const val = try stmt.column(0);
            try res.append(self.db.arena, try val.into(T, self.db.arena));
        }
        return res.toOwnedSlice(self.db.arena);
    }

    pub fn fetchOne(self: RawQuery, comptime T: type) !?T {
        var stmt = try self.prepare(util.columns(T));
        defer stmt.deinit();

        if (!try stmt.step()) return null;
        return try readRow(T, &stmt, self.db.arena);
    }

    pub fn fetchAll(self: RawQuery, comptime T: type) ![]const T {
        var stmt = try self.prepare(util.columns(T));
        defer stmt.deinit();

        var res: std.ArrayList(T) = .empty;
        while (try stmt.step()) {
            const item = try readRow(T, &stmt, self.db.arena);
            try res.append(self.db.arena, item);
        }

        return res.toOwnedSlice(self.db.arena);
    }

    fn readRow(comptime T: type, stmt: *Statement, arena: std.mem.Allocator) !T {
        const col_count = stmt.columnCount();
        var res: T = undefined;

        if (@typeInfo(T).@"struct".is_tuple) {
            inline for (comptime std.meta.fieldTypes(T), 0..) |ft, idx| {
                res[idx] = try (try stmt.column(idx)).into(ft, arena);
            }
        } else {
            inline for (comptime std.meta.fieldNames(T), comptime std.meta.fieldTypes(T)) |f, ft| {
                // TODO: this is wrong, we should prepare mapping table once, and then use it for all rows
                const idx = findColumnIndex(stmt, col_count, f) orelse return error.MissingColumn;
                @field(res, f) = try (try stmt.column(idx)).into(ft, arena);
            }
        }

        return res;
    }

    fn findColumnIndex(stmt: *Statement, col_count: usize, name: []const u8) ?usize {
        for (0..col_count) |i| {
            if (std.mem.eql(u8, stmt.columnName(i), name)) return i;
        }

        return null;
    }

    pub fn prepare(self: RawQuery, columns: []const u8) !Statement {
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
                if (a.kind == .raw or a.kind == .ident) return false;
                if (b.kind == .raw or b.kind == .ident) return false;
                return a.order() < b.order();
            }
        };
        std.sort.insertion(*Part, parts, {}, H.lt);

        // Prepare writer (TODO: could we re-use this for multiple queries?)
        var aw: std.Io.Writer.Allocating = try .initCapacity(self.db.arena, 256);
        const w = &aw.writer;

        // Render prefix, if there is any
        switch (self.prefix) {
            .none => {},
            .select => try w.print("SELECT {s} FROM", .{columns}),
            inline else => |t| try w.writeAll(@tagName(t)),
        }

        // Render in sorted order, tracking prev kind for proper separators
        var prev_kind: ?Part.Kind = null;
        for (parts) |part| {
            if (part.kind == .ident) {
                try w.printStringEscaped(part.sql);
                continue;
            } else if (part.kind != .raw) {
                const comma = switch (part.kind) {
                    .SELECT, .@"GROUP BY", .@"ORDER BY", .SET => prev_kind != null and prev_kind.? == part.kind,
                    else => false,
                };

                try w.writeAll(if (comma) ", " else switch (part.kind) {
                    .table => " ",
                    .cols => "",
                    // TODO: maybe we could add `has_where` flag and the builder method itself could append different part kind (like we did before)
                    .WHERE => if (prev_kind == .WHERE or prev_kind == .OR) " AND " else " WHERE ",
                    .OR => if (prev_kind == .WHERE or prev_kind == .OR) " OR " else " WHERE ",
                    inline else => |t| " " ++ @tagName(t) ++ " ",
                });
            }

            try w.writeAll(part.sql);
            if (part.kind == .SELECT) try w.writeAll(" FROM ");
            prev_kind = part.kind;
        }

        var stmt: Statement = try self.db.conn.prepare(aw.written());
        errdefer stmt.deinit();

        // Bind args
        var i: usize = 0;
        for (parts) |part| {
            for (self.db.args.items[part.args[0]..part.args[1]]) |arg| {
                try stmt.bind(i, arg);
                i += 1;
            }
        }

        return stmt;
    }

    pub fn append(self: RawQuery, kind: Part.Kind, sql: []const u8, args: anytype) RawQuery {
        const bit = self.db.parts.items.len - self.begin;
        const args_range = self.db.pushArgs(args) catch @panic("OOM/error"); // TODO: introduce Value.error?
        // TODO: we could use mask=0 to track OOM queries and return error.OutOfMemory in prepare()
        self.db.parts.append(self.db.arena, .{ .kind = kind, .sql = sql, .args = args_range }) catch @panic("OOM");
        var copy = self;
        copy.mask |= @as(u64, 1) << @intCast(bit);
        return copy;
    }
};

const expectSql = @import("testing.zig").expectSql;
const fakeDb = @import("testing.zig").fakeDb;

test "select" {
    var db = try fakeDb();
    defer db.deinit();
    const select1 = RawQuery.init(&db).select("1");

    try expectSql(select1, "SELECT 1");
    try expectSql(select1.select("2"), "SELECT 1, 2");
    try expectSql(select1.selectRaw("?", 1), "SELECT 1, ?");
}

test "insert" {
    var db = try fakeDb();
    defer db.deinit();
    const insert = RawQuery.init(&db).insert();

    try expectSql(insert, "INSERT INTO");
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

    try expectSql(delete, "DELETE FROM");
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
