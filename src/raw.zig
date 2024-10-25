const std = @import("std");
const Session = @import("session.zig").Session;
const Value = @import("value.zig").Value;
const Statement = @import("statement.zig").Statement;
const util = @import("util.zig");

const Part = struct {
    prev: ?*const Part = null,
    kind: Kind,
    sql: []const u8 = "",
    args: []const Value = &.{},

    pub const Kind = enum { raw, SELECT, JOIN, @"LEFT JOIN", WHERE, AND, OR, @"GROUP BY", HAVING, @"ORDER BY", VALUES, SET, @"ON CONFLICT" };

    fn toSql(self: Part, buf: *std.ArrayList(u8)) !void {
        if (self.prev) |p| try p.toSql(buf);

        if (self.kind != .raw) {
            const comma = switch (self.kind) {
                .SELECT, .@"GROUP BY", .@"ORDER BY", .SET => self.prev != null and self.prev.?.kind == self.kind,
                else => false,
            };

            try buf.appendSlice(if (comma) ", " else switch (self.kind) {
                .SELECT => "SELECT ",
                inline else => |t| " " ++ @tagName(t) ++ " ",
            });
        }

        try buf.appendSlice(self.sql);
    }

    fn bind(self: Part, stmt: anytype, i: *usize) !void {
        if (self.prev) |p| try p.bind(stmt, i);

        for (self.args) |arg| {
            try stmt.bind(i.*, arg);
            i.* += 1;
        }
    }
};

pub const Query = struct {
    session: *Session,
    parts: struct {
        head: ?*const Part = null, //   SELECT, <raw>
        tables: ?*const Part = null, // <raw>, JOIN
        where: ?*const Part = null, //  WHERE, AND, OR
        tail: ?*const Part = null, //   everything else
    } = .{},

    pub fn init(session: *Session) Query {
        return .{ .session = session };
    }

    pub fn raw(session: *Session, sql: []const u8, args: anytype) Query {
        return init(session).append("head", .raw, sql, args);
    }

    pub fn insert(self: Query) Query {
        return self.reset("head", comptime &.{ .kind = .raw, .sql = "INSERT" });
    }

    pub fn cols(self: Query, sql: []const u8) Query {
        return self.append("tables", .raw, sql, .{});
    }

    pub fn values(self: Query, sql: []const u8, args: anytype) Query {
        return self.append("tables", .VALUES, sql, args);
    }

    pub fn onConflict(self: Query, sql: []const u8, args: anytype) Query {
        return self.append("tail", .@"ON CONFLICT", sql, args);
    }

    pub fn update(self: Query) Query {
        return self.reset("head", comptime &.{ .kind = .raw, .sql = "UPDATE" });
    }

    pub fn set(self: Query, sql: []const u8, args: anytype) Query {
        return self.append("tables", .SET, sql, args);
    }

    pub fn setAll(self: Query, data: anytype) Query {
        if (comptime std.meta.fields(@TypeOf(data)).len == 0) {
            return self;
        }

        return self.append("tables", .SET, util.setters(@TypeOf(data)), data);
    }

    pub fn delete(self: Query) Query {
        return self.reset("head", comptime &.{ .kind = .raw, .sql = "DELETE" });
    }

    pub fn reselect(self: Query, sql: []const u8) Query {
        const part = self.session.arena.create(Part) catch @panic("OOM");
        part.* = .{ .kind = .SELECT, .sql = sql };
        return self.reset("head", part);
    }

    pub fn select(self: Query, sql: []const u8) Query {
        return self.append("head", .SELECT, sql, .{});
    }

    pub fn table(self: Query, sql: []const u8) Query {
        const part = self.session.arena.create(Part) catch @panic("OOM");
        part.* = .{ .kind = .raw, .sql = sql };
        return self.reset("tables", part);
    }

    pub const from = table;
    pub const into = table;

    pub fn join(self: Query, sql: []const u8) Query {
        return self.append("tables", .JOIN, sql, .{});
    }

    pub fn leftJoin(self: Query, sql: []const u8) Query {
        return self.append("tables", .@"LEFT JOIN", sql, .{});
    }

    pub fn where(self: Query, sql: []const u8, args: anytype) Query {
        return self.append("where", if (self.parts.where == null) .WHERE else .AND, sql, args);
    }

    pub fn orWhere(self: Query, sql: []const u8, args: anytype) Query {
        return self.append("where", if (self.parts.where == null) .WHERE else .OR, sql, args);
    }

    pub fn groupBy(self: Query, sql: []const u8) Query {
        return self.append("tail", .@"GROUP BY", sql, .{});
    }

    pub fn having(self: Query, sql: []const u8, args: anytype) Query {
        return self.append("tail", .HAVING, sql, args);
    }

    pub fn orderBy(self: Query, sql: []const u8) Query {
        return self.append("tail", .@"ORDER BY", sql, .{});
    }

    pub fn limit(self: Query, n: i32) Query {
        return self.append("tail", .raw, " LIMIT ?", .{n});
    }

    pub fn offset(self: Query, i: i32) Query {
        return self.append("tail", .raw, " OFFSET ?", .{i});
    }

    pub fn exec(self: Query) !void {
        var stmt = try self.prepare();
        defer stmt.deinit();

        try stmt.exec();
    }

    pub fn get(self: Query, comptime T: type) !?T {
        var stmt = try self.prepare();
        defer stmt.deinit();

        if (try stmt.next(struct { T }, self.session.arena)) |row| {
            return row[0];
        }

        return null;
    }

    pub fn fetchOne(self: Query, comptime R: type) !?R {
        var stmt = try self.prepare();
        defer stmt.deinit();

        return stmt.next(R, self.session.arena);
    }

    pub fn fetchAll(self: Query, comptime R: type) ![]const R {
        var stmt = try self.prepare();
        defer stmt.deinit();

        var res = std.ArrayList(R).init(self.session.arena);
        errdefer res.deinit();

        while (try stmt.next(R, self.session.arena)) |row| {
            try res.append(row);
        }

        return res.toOwnedSlice();
    }

    pub fn toSql(self: Query, buf: *std.ArrayList(u8)) !void {
        if (self.parts.head) |h| try h.toSql(buf);

        if (self.parts.tables) |t| {
            try buf.appendSlice(switch (std.ascii.toLower(buf.items[0])) {
                'i' => " INTO ",
                's', 'd' => " FROM ",
                else => " ",
            });

            try t.toSql(buf);
        }

        if (self.parts.where) |w| try w.toSql(buf);
        if (self.parts.tail) |t| try t.toSql(buf);
    }

    pub fn prepare(self: Query) !Statement {
        var buf = try std.ArrayList(u8).initCapacity(self.session.arena, 256);
        try self.toSql(&buf);

        var stmt = try self.session.prepare(buf.items, .{});
        errdefer stmt.deinit();

        var i: usize = 0;
        if (self.parts.head) |p| try p.bind(&stmt, &i);
        if (self.parts.tables) |t| try t.bind(&stmt, &i);
        if (self.parts.where) |w| try w.bind(&stmt, &i);
        if (self.parts.tail) |t| try t.bind(&stmt, &i);

        return stmt;
    }

    fn append(self: Query, comptime slot: []const u8, kind: Part.Kind, sql: []const u8, args: anytype) Query {
        const part = self.session.arena.create(Part) catch @panic("OOM");
        part.* = .{ .prev = @field(self.parts, slot), .kind = kind, .sql = sql, .args = toValues(self.session, args) };
        return reset(self, slot, part);
    }

    fn reset(self: Query, comptime slot: []const u8, part: *const Part) Query {
        var copy = self;
        @field(copy.parts, slot) = part;
        return copy;
    }

    fn toValues(session: *Session, args: anytype) []const Value {
        const fields = @typeInfo(@TypeOf(args)).@"struct".fields;

        if (fields.len == 0) {
            return &.{};
        }

        const res = session.arena.alloc(Value, fields.len) catch @panic("OOM");

        inline for (fields, 0..) |f, i| {
            res[i] = Value.from(@field(args, f.name), session.arena) catch @panic("OOM");
        }

        return res;
    }
};
