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
        return self.append("constraints", .{ .primary_key = cols });
    }

    pub fn unique(self: *TableBuilder, cols: []const u8) *TableBuilder {
        return self.append("constraints", .{ .unique = cols });
    }

    pub fn check(self: *TableBuilder, expr: []const u8) *TableBuilder {
        return self.append("constraints", .{ .check = expr });
    }

    pub fn foreignKey(self: *TableBuilder, cols: []const u8, table: []const u8, opts: FkOptions) *TableBuilder {
        const fk: Fk = .{
            .cols = cols,
            .ref_table = table,
            .ref_cols = opts.columns,
            .on_delete = opts.on_delete,
            .on_update = opts.on_update,
        };

        return self.append("constraints", .{ .foreign_key = fk });
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
    changes: std.ArrayListUnmanaged(TableChange) = .{},

    pub fn addColumn(self: *AlterBuilder, name: []const u8, ctype: ColumnType, opts: ColumnOptions) *AlterBuilder {
        if (opts.primary_key) {
            // TODO
            // _ = self.addPrimaryKey(name);
        }

        if (opts.unique) {
            // TODO
            // _ = self.addUnique(name);
        }

        return self.append(.{ .add_column = .{ name, ctype, opts } });
    }

    pub fn renameColumn(self: *AlterBuilder, old_name: []const u8, new_name: []const u8) *AlterBuilder {
        return self.append(.{ .rename_column = .{ old_name, new_name } });
    }

    pub fn dropColumn(self: *AlterBuilder, name: []const u8) *AlterBuilder {
        return self.append(.{ .drop_column = name });
    }

    fn append(self: *AlterBuilder, change: TableChange) *AlterBuilder {
        self.changes.append(self.db.arena, change) catch @panic("OOM");
        return self;
    }

    pub fn exec(self: *AlterBuilder) !void {
        // const sqlite = std.mem.eql(u8, self.db.conn.kind(), "sqlite3");

        // if (sqlite and !empty) {
        //     var tws = try TwelveStep.init(self.db, self.table);
        //     for (self.changes.items) |ch| try tws.apply(ch);
        //     try tws.exec();
        // } else {
        var buf = try SqlBuf.init(self.db.arena);
        try buf.append(self);
        try self.db.conn.execAll(buf.buf.items);
        // }
    }

    pub fn toSql(self: *AlterBuilder, buf: *SqlBuf) !void {
        for (self.changes.items, 0..) |ch, i| {
            if (i > 0) try buf.append(";\n");

            try buf.append("ALTER TABLE ");
            try buf.append(self.table);
            try buf.append(" ");
            try buf.append(ch);
        }
    }
};

pub const TableChange = union(enum) {
    add_column: struct { []const u8, ColumnType, ColumnOptions },
    rename_column: struct { []const u8, []const u8 },
    drop_column: []const u8,

    pub fn toSql(self: TableChange, buf: *SqlBuf) !void {
        switch (self) {
            .add_column => |opts| {
                try buf.append("ADD COLUMN ");
                try buf.append(opts[0]);
                try buf.append(" ");
                try buf.append(opts[1]);

                if (!opts[2].nullable) {
                    try buf.append(" NOT NULL");
                }
            },
            .rename_column => |names| {
                try buf.append("RENAME COLUMN ");
                try buf.append(names[0]);
                try buf.append(" TO ");
                try buf.append(names[1]);
            },
            .drop_column => |name| {
                try buf.append("DROP COLUMN ");
                try buf.append(name);
            },
        }
    }
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

pub const Constraint = union(enum) {
    primary_key: []const u8,
    unique: []const u8,
    check: []const u8,
    foreign_key: Fk,

    pub fn toSql(self: Constraint, buf: *SqlBuf) !void {
        try buf.append(switch (self) {
            .primary_key => "PRIMARY KEY",
            .unique => "UNIQUE",
            .check => "CHECK",
            .foreign_key => "FOREIGN KEY",
        });

        switch (self) {
            .foreign_key => |fk| try buf.append(fk),
            inline else => |v| {
                try buf.append(" (");
                try buf.append(v);
                try buf.append(")");
            },
        }
    }
};

const Fk = struct {
    cols: []const u8,
    ref_table: []const u8,
    ref_cols: []const u8,
    on_update: FkAction = .no_action,
    on_delete: FkAction = .no_action,

    pub fn toSql(self: Fk, buf: *SqlBuf) !void {
        try buf.append(" (");
        try buf.append(self.cols);
        try buf.append(") REFERENCES ");
        try buf.append(self.ref_table);
        try buf.append(" (");
        try buf.append(self.ref_cols);
        try buf.append(")");

        if (self.on_update != .no_action) {
            try buf.append(" ON UPDATE ");
            try buf.append(self.on_update);
        }

        if (self.on_delete != .no_action) {
            try buf.append(" ON DELETE ");
            try buf.append(self.on_delete);
        }
    }
};

pub const FkAction = enum {
    set_null,
    set_default,
    cascade,
    restrict,
    no_action,

    pub fn toSql(self: FkAction, buf: *SqlBuf) !void {
        try buf.append(switch (self) {
            .set_null => "SET NULL",
            .set_default => "SET DEFAULT",
            .cascade => "CASCADE",
            .restrict => "RESTRICT",
            .no_action => "NO ACTION",
        });
    }
};

pub const FkOptions = struct {
    columns: []const u8 = "id",
    on_delete: FkAction = .no_action,
    on_update: FkAction = .no_action,
};

const t = std.testing;
const createDb = @import("testing.zig").createDb;
const expectSql = @import("testing.zig").expectSql;
const expectDdl = @import("testing.zig").expectDdl;

test "basic create" {
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

test "basic alter" {
    var db = try createDb("");
    defer db.deinit();
    const schema = db.schema();

    try schema.createTable("person")
        .id()
        .column("age", .int, .{})
        .exec();

    try schema.alterTable("person")
        .addColumn("name", .text, .{})
        .dropColumn("age")
        .exec();

    try expectDdl(&db, "person",
        \\CREATE TABLE person (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  PRIMARY KEY (id)
        \\) STRICT
    );

    try schema.dropTable("person");
}

test "advanced create" {
    var db = try createDb("");
    defer db.deinit();
    const schema = db.schema();

    try schema.createTable("employee")
        .id()
        .column("name", .text, .{})
        .column("department_id", .int, .{})
        .foreignKey("department_id", "department", .{})
        .exec();

    try schema.createTable("department")
        .id()
        .column("name", .text, .{})
        .exec();

    try expectDdl(&db, "employee",
        \\CREATE TABLE employee (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  department_id INTEGER NOT NULL,
        \\  PRIMARY KEY (id),
        \\  FOREIGN KEY (department_id) REFERENCES department (id)
        \\) STRICT
    );

    try expectDdl(&db, "department",
        \\CREATE TABLE department (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  PRIMARY KEY (id)
        \\) STRICT
    );

    try schema.dropTable("department");
    try schema.dropTable("employee");
}
