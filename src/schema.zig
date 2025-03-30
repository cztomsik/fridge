const std = @import("std");
const Session = @import("session.zig").Session;
const RawQuery = @import("raw.zig").Query;
const SqlBuf = @import("sql.zig").SqlBuf;

pub const Schema = struct {
    db: *Session,

    pub fn init(db: *Session) Schema {
        return .{ .db = db };
    }

    pub fn createTable(self: Schema, name: []const u8) *TableBuilder {
        const res = self.db.arena.create(TableBuilder) catch @panic("OOM");
        res.* = .{ .db = self.db, .table = name };
        return res;
    }

    pub fn alterTable(self: Schema, name: []const u8) *AlterBuilder {
        const res = self.db.arena.create(AlterBuilder) catch @panic("OOM");
        res.* = .{ .db = self.db, .table = name };
        return res;
    }

    pub fn renameTable(self: Schema, old_name: []const u8, new_name: []const u8) !void {
        const sql = try std.fmt.allocPrint(self.db.arena, "ALTER TABLE {s} RENAME TO {s}", .{ old_name, new_name });
        try self.db.conn.execAll(sql);
    }

    pub fn dropTable(self: Schema, name: []const u8) !void {
        try self.db.raw("DROP TABLE", {}).table(name).exec();
    }
};

pub const TableBuilder = struct {
    db: *Session,
    table: []const u8,
    columns: std.ArrayListUnmanaged(Column) = .{},
    constraints: std.ArrayListUnmanaged(Constraint) = .{},

    pub fn id(self: *TableBuilder) *TableBuilder {
        return self.column("id", ColumnType.int, .{
            .primary_key = true,
        });
    }

    pub fn column(self: *TableBuilder, name: []const u8, ctype: ColumnType, opts: ColumnOptions) *TableBuilder {
        const col: Column = .{
            .name = name,
            .type = ctype,
            .not_null = !opts.nullable and !opts.primary_key,
            .default = opts.default,
        };

        if (opts.primary_key) {
            _ = self.primaryKey(name);
        }

        if (opts.unique) {
            _ = self.unique(name);
        }

        return self.append("columns", col);
    }

    pub fn primaryKey(self: *TableBuilder, cols: []const u8) *TableBuilder {
        return self.append("constraints", .{
            .type = ConstraintType.primary_key,
            .body = cols,
        });
    }

    pub fn unique(self: *TableBuilder, cols: []const u8) *TableBuilder {
        return self.append("constraints", .{
            .type = ConstraintType.unique,
            .body = cols,
        });
    }

    pub fn toSql(self: TableBuilder, buf: *SqlBuf) !void {
        // TODO: db.raw() should be flexible-enough for this

        try buf.append("CREATE TABLE ");
        try buf.append(self.table);
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
        var buf = try SqlBuf.init(self.db.arena);
        try buf.append(self);
        try self.db.conn.execAll(buf.buf.items);
    }

    fn append(self: *TableBuilder, comptime slot: []const u8, item: std.meta.Child(@FieldType(TableBuilder, slot).Slice)) *TableBuilder {
        @field(self, slot).append(self.db.arena, item) catch @panic("OOM");
        return self;
    }
};

pub const AlterBuilder = struct {
    db: *Session,
    table: []const u8,
    // changes: std.ArrayListUnmanaged(TableChange) = .{},

};

pub const TableChange = union(enum) {
};

pub const Column = struct {
    name: []const u8,
    type: ColumnType,
    not_null: bool,
    default: ?[]const u8,

    pub fn toSql(self: Column, buf: *SqlBuf) !void {
        try buf.append(self.name);
        try buf.append(" ");
        try buf.append(self.type);

        if (self.not_null) {
            try buf.append(" NOT NULL");
        }

        if (self.default) |def| {
            try buf.append(" DEFAULT ");
            try buf.append(def);
        }
    }
};

pub const ColumnType = enum {
    integer,
    text,

    pub const int = ColumnType.integer;

    pub fn toSql(self: ColumnType, buf: *SqlBuf) !void {
        try buf.append(switch (self) {
            .integer => "INTEGER",
            .text => "TEXT",
        });
    }
};

pub const ColumnOptions = struct {
    nullable: bool = false,
    primary_key: bool = false,
    unique: bool = false,
    default: ?[]const u8 = null,
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

test "basic usage" {
    var db = try createDb("");
    defer db.deinit();
    const schema = db.schema();

    try schema.createTable("person")
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

    try schema.dropTable("person");
}

