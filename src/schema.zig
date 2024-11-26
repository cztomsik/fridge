const std = @import("std");
const Session = @import("session.zig").Session;
const RawQuery = @import("raw.zig").Query;
const SqlBuf = @import("sql.zig").SqlBuf;

pub const Schema = struct {
    session: *Session,

    pub fn init(session: *Session) Schema {
        return .{ .session = session };
    }

    pub fn createTable(self: *const Schema, name: []const u8) *TableBuilder {
        const res = self.session.arena.create(TableBuilder) catch @panic("OOM");
        res.* = .{ .schema = self, .name = name };
        return res;
    }

    pub fn dropTable(self: *const Schema, name: []const u8) !void {
        try self.session.raw("DROP TABLE", .{}).table(name).exec();
    }
};

pub const TableBuilder = struct {
    schema: *const Schema,
    name: []const u8,
    columns: std.ArrayListUnmanaged(Column) = .{},
    constraints: std.ArrayListUnmanaged(Constraint) = .{},

    pub fn id(self: *TableBuilder) *TableBuilder {
        return self.column("id", ColumnType.int, .{ .primary_key = true });
    }

    pub fn column(self: *TableBuilder, name: []const u8, ctype: ColumnType, opts: ColumnOptions) *TableBuilder {
        const col: Column = .{ .name = name, .type = ctype, .not_null = !opts.nullable and !opts.primary_key };
        if (opts.primary_key) _ = self.primaryKey(name);
        if (opts.unique) _ = self.unique(name);
        return self.append("columns", col);
    }

    pub fn primaryKey(self: *TableBuilder, cols: []const u8) *TableBuilder {
        return self.append("constraints", .{ .type = ConstraintType.primary_key, .body = cols });
    }

    pub fn unique(self: *TableBuilder, cols: []const u8) *TableBuilder {
        return self.append("constraints", .{ .type = ConstraintType.unique, .body = cols });
    }

    pub fn toSql(self: TableBuilder, buf: *SqlBuf) !void {
        // TODO: db.raw() should be flexible-enough for this

        try buf.append("CREATE TABLE ");
        try buf.append(self.name);
        try buf.append(" (\n  ");

        for (self.columns.items, 0..) |col, i| {
            if (i > 0) try buf.append(",\n  ");
            try buf.append(col);
        }

        for (self.constraints.items) |con| {
            try buf.append(",\n  ");
            try buf.append(con);
        }

        // TODO: if (self.dialect == .sqlite) or something like that
        try buf.append("\n) STRICT");
    }

    pub fn exec(self: TableBuilder) !void {
        var buf = try SqlBuf.init(self.schema.session.arena);
        try buf.append(self);
        try self.schema.session.exec(buf.buf.items, .{});
    }

    fn append(self: *TableBuilder, comptime slot: []const u8, item: std.meta.Child(@FieldType(TableBuilder, slot).Slice)) *TableBuilder {
        @field(self, slot).append(self.schema.session.arena, item) catch @panic("OOM");
        return self;
    }
};

pub const Column = struct {
    name: []const u8,
    type: ColumnType,
    not_null: bool,

    pub fn toSql(self: Column, buf: *SqlBuf) !void {
        try buf.append(self.name);
        try buf.append(" ");
        try buf.append(self.type);
        if (self.not_null) try buf.append(" NOT NULL");
    }
};

pub const ColumnType = enum {
    int,
    text,

    pub fn toSql(self: ColumnType, buf: *SqlBuf) !void {
        try buf.append(switch (self) {
            .int => "INTEGER",
            .text => "TEXT",
        });
    }
};

pub const ColumnOptions = struct {
    nullable: bool = false,
    primary_key: bool = false,
    unique: bool = false,
};

pub const Constraint = struct {
    type: ConstraintType,
    body: []const u8,

    pub fn toSql(self: Constraint, buf: *SqlBuf) !void {
        try buf.append(self.type);
        try buf.append(" (");
        try buf.append(self.body);
        try buf.append(")");
    }
};

pub const ConstraintType = enum {
    primary_key,
    unique,

    pub fn toSql(self: ConstraintType, buf: *SqlBuf) !void {
        try buf.append(switch (self) {
            .primary_key => "PRIMARY KEY",
            .unique => "UNIQUE",
        });
    }
};

const t = std.testing;
const createDb = @import("testing.zig").createDb;
const expectDdl = @import("testing.zig").expectDdl;

test {
    const Person = struct { id: ?u32 = null, name: []const u8, age: u32 };

    var db = try createDb("");
    defer db.deinit();

    try db.schema().createTable("person")
        .id()
        .column("name", .text, .{})
        .column("age", .int, .{})
        .exec();

    try expectDdl(&db, "person",
        \\CREATE TABLE person (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  age INTEGER NOT NULL,
        \\  PRIMARY KEY (id)
        \\) STRICT
    );

    const data = try db.create(Person, .{ .name = "Alice", .age = 30 });
    std.debug.print("{any}\n", .{data});

    try db.schema().dropTable("person");
}
