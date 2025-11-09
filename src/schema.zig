const std = @import("std");
const util = @import("util.zig");
const Value = @import("value.zig").Value;
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
        var buf = try SqlBuf.init(self.db.arena);
        try buf.append("ALTER TABLE ");
        try buf.appendIdent(old_name);
        try buf.append(" RENAME TO ");
        try buf.appendIdent(new_name);

        try self.db.conn.execAll(buf.buf.items);
    }

    pub fn dropTable(self: Schema, name: []const u8) !void {
        var buf = try SqlBuf.init(self.db.arena);
        try buf.append("DROP TABLE ");
        try buf.appendIdent(name);

        try self.db.conn.execAll(buf.buf.items);
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
        return self.addConstraint(.primary_key, cols);
    }

    pub fn unique(self: *TableBuilder, cols: []const u8) *TableBuilder {
        return self.addConstraint(.unique, cols);
    }

    pub fn check(self: *TableBuilder, expr: []const u8) *TableBuilder {
        return self.addConstraint(.check, expr);
    }

    pub fn foreignKey(self: *TableBuilder, cols: []const u8, table: []const u8, opts: FkOptions) *TableBuilder {
        return self.addConstraint(.foreign_key, .init(cols, table, opts));
    }

    pub fn addConstraint(self: *TableBuilder, comptime kind: std.meta.Tag(Constraint), body: @FieldType(Constraint, @tagName(kind))) *TableBuilder {
        return self.append("constraints", @unionInit(Constraint, @tagName(kind), body));
    }

    pub fn toSql(self: TableBuilder, buf: *SqlBuf) !void {
        try buf.append("CREATE TABLE ");
        try buf.appendIdent(self.table);
        try buf.append(" (\n  ");

        for (self.columns.items, 0..) |col, i| {
            if (i > 0) try buf.append(",\n  ");
            try buf.append(col);
        }

        // Fixed order
        for (std.meta.tags(std.meta.Tag(Constraint))) |tag| {
            for (self.constraints.items) |con| {
                if (std.meta.activeTag(con) == tag) {
                    try buf.append(",\n  ");
                    try buf.append(con);
                }
            }
        }

        if (self.db.conn.dialect() == .sqlite3) {
            try buf.append("\n) STRICT");
        }
    }

    pub fn exec(self: *TableBuilder) !void {
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
            _ = self.addPrimaryKey(name);
        }

        if (opts.unique) {
            _ = self.addUnique(name);
        }

        return self.append(.{ .add_column = .{ name, ctype, opts } });
    }

    pub fn renameColumn(self: *AlterBuilder, old_name: []const u8, new_name: []const u8) *AlterBuilder {
        return self.append(.{ .rename_column = .{ old_name, new_name } });
    }

    pub fn dropColumn(self: *AlterBuilder, name: []const u8) *AlterBuilder {
        return self.append(.{ .drop_column = name });
    }

    pub fn addPrimaryKey(self: *AlterBuilder, cols: []const u8) *AlterBuilder {
        return self.addConstraint(.primary_key, cols);
    }

    pub fn addUnique(self: *AlterBuilder, cols: []const u8) *AlterBuilder {
        return self.addConstraint(.unique, cols);
    }

    pub fn addCheck(self: *AlterBuilder, expr: []const u8) *AlterBuilder {
        return self.addConstraint(.check, expr);
    }

    pub fn addForeignKey(self: *AlterBuilder, cols: []const u8, table: []const u8, opts: FkOptions) *AlterBuilder {
        return self.addConstraint(.foreign_key, .init(cols, table, opts));
    }

    pub fn addConstraint(self: *AlterBuilder, comptime kind: std.meta.Tag(Constraint), body: @FieldType(Constraint, @tagName(kind))) *AlterBuilder {
        return self.append(.{ .add_constraint = @unionInit(Constraint, @tagName(kind), body) });
    }

    pub fn dropPrimaryKey(self: *AlterBuilder) *AlterBuilder {
        return self.append(.{ .drop_constraint = .{ .primary_key = {} } });
    }

    pub fn dropUnique(self: *AlterBuilder, cols: []const u8) *AlterBuilder {
        return self.append(.{ .drop_constraint = .{ .unique = cols } });
    }

    pub fn dropCheck(self: *AlterBuilder, expr: []const u8) *AlterBuilder {
        return self.append(.{ .drop_constraint = .{ .check = expr } });
    }

    pub fn dropForeignKey(self: *AlterBuilder, cols: []const u8) *AlterBuilder {
        return self.append(.{ .drop_constraint = .{ .foreign_key = cols } });
    }

    fn append(self: *AlterBuilder, change: TableChange) *AlterBuilder {
        self.changes.append(self.db.arena, change) catch @panic("OOM");
        return self;
    }

    fn needsTwelveStep(self: AlterBuilder) bool {
        for (self.changes.items) |change| {
            switch (change) {
                .add_column => |opts| {
                    if (!opts[2].nullable and opts[2].default == null) {
                        return true;
                    }
                },
                .rename_column => {
                    continue;
                },
                // There are far too many restrictions, see:
                // https://www.sqlite.org/lang_altertable.html
                .drop_column, .add_constraint, .drop_constraint => {
                    return true;
                },
            }
        }
        return false;
    }

    pub fn exec(self: *AlterBuilder) !void {
        const sqlite = self.db.conn.dialect() == .sqlite3;
        const needs_twelvestep = self.needsTwelveStep();

        if (sqlite and needs_twelvestep) {
            var tws = try TwelveStep.init(self.db, self.table, self.changes.items);
            try tws.exec();
        } else {
            var buf = try SqlBuf.init(self.db.arena);
            try buf.append(self);
            try self.db.conn.execAll(buf.buf.items);
        }
    }

    pub fn toSql(self: AlterBuilder, buf: *SqlBuf) !void {
        for (self.changes.items, 0..) |ch, i| {
            if (i > 0) try buf.append(";\n");

            try buf.append("ALTER TABLE ");
            try buf.appendIdent(self.table);
            try buf.append(" ");
            try buf.append(ch);
        }
    }
};

pub const TableChange = union(enum) {
    add_column: struct { []const u8, ColumnType, ColumnOptions },
    rename_column: struct { []const u8, []const u8 },
    drop_column: []const u8,
    add_constraint: Constraint,
    drop_constraint: union(enum) {
        primary_key,
        unique: []const u8,
        check: []const u8,
        foreign_key: []const u8,
    },

    pub fn toSql(self: TableChange, buf: *SqlBuf) !void {
        switch (self) {
            .add_column => |opts| {
                try buf.append("ADD COLUMN ");
                try buf.appendIdent(opts[0]);
                try buf.append(" ");
                try buf.append(opts[1]);

                if (!opts[2].nullable) {
                    try buf.append(" NOT NULL");
                }

                if (opts[2].default) |def| {
                    try buf.append(" DEFAULT ");
                    try buf.append(def);
                }
            },
            .rename_column => |names| {
                try buf.append("RENAME COLUMN ");
                try buf.appendIdent(names[0]);
                try buf.append(" TO ");
                try buf.appendIdent(names[1]);
            },
            .drop_column => |name| {
                try buf.append("DROP COLUMN ");
                try buf.appendIdent(name);
            },
            .add_constraint => |constraint| {
                try buf.append("ADD CONSTRAINT ");
                try buf.append(constraint);
            },
            .drop_constraint => |drop| {
                try buf.append("DROP CONSTRAINT ");
                switch (drop) {
                    .primary_key => try buf.append("PRIMARY KEY"),
                    .unique => |name| {
                        try buf.append("UNIQUE (");
                        try buf.append(name);
                        try buf.append(")");
                    },
                    .check => |expr| {
                        try buf.append("CHECK (");
                        try buf.append(expr);
                        try buf.append(")");
                    },
                    .foreign_key => |cols| {
                        try buf.append("FOREIGN KEY (");
                        try buf.append(cols);
                        try buf.append(")");
                    },
                }
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
        try buf.append(self.name); // TODO: should be appendIdent() but we probably don't want to quote unless necessary
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

    fn refersOnly(self: Constraint, col: []const u8) bool {
        const cols = switch (self) {
            inline else => |body| body,
            .foreign_key => |fk| fk.cols,
        };

        if (std.mem.eql(u8, cols, col)) {
            return true;
        }

        // CHECK xxx <op> xxx
        return util.isSimpleExpr(cols) and
            cols.len > col.len and cols[col.len] == ' ' and
            std.mem.startsWith(u8, cols, col);
    }

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

    fn init(cols: []const u8, table: []const u8, opts: FkOptions) Fk {
        return .{
            .cols = cols,
            .ref_table = table,
            .ref_cols = opts.columns,
            .on_delete = opts.on_delete,
            .on_update = opts.on_update,
        };
    }

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

    pub fn fromValue(v: Value, _: std.mem.Allocator) !FkAction {
        switch (v) {
            .string => |str| {
                if (str.len <= 11) {
                    var buf: [11]u8 = undefined;
                    const name = std.ascii.lowerString(&buf, str);
                    std.mem.replaceScalar(u8, name, ' ', '_');
                    return std.meta.stringToEnum(FkAction, name) orelse error.InvalidEnumTag;
                }
            },
            else => {},
        }

        return error.InvalidEnumTag;
    }

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

pub const TwelveStep = struct {
    db: *Session,
    table: []const u8,
    changes: []const TableChange,
    state: TableBuilder,

    const TEMP_TABLE: []const u8 = "temp_alter_table";

    pub fn init(db: *Session, table: []const u8, changes: []const TableChange) !TwelveStep {
        var state: TableBuilder = .{
            .db = db,
            .table = TEMP_TABLE,
        };

        const sql = try db
            .raw("SELECT sql FROM sqlite_master WHERE tbl_name = ?", table)
            .get([]const u8) orelse return error.NotFound;

        const cols = try db
            .raw("SELECT name, lower(type) type, \"notnull\" not_null, dflt_value \"default\" FROM pragma_table_xinfo(?)", table)
            .fetchAll(Column);

        for (cols) |col| {
            _ = state.append("columns", col);
        }

        // PRIMARY KEY
        if (try db.raw("SELECT group_concat(name, ',') FROM pragma_table_xinfo(?) WHERE pk = 1", table).get([]const u8)) |pk_cols| {
            _ = state.addConstraint(.primary_key, pk_cols);
        }

        // UNIQUE
        for (try db.raw("SELECT group_concat(c.name, ',') FROM pragma_index_list(?) i JOIN pragma_index_info(i.name) c WHERE i.\"unique\" = 1 GROUP BY i.name", table).fetchAll([]const u8)) |uq_cols| {
            _ = state.addConstraint(.unique, uq_cols);
        }

        // CHECK (parse from the original CREATE TABLE)
        var pos: usize = 0;
        while (std.mem.indexOfPos(u8, sql, pos, "CHECK (")) |i| {
            const eol = std.mem.indexOfScalarPos(u8, sql, i, '\n') orelse return error.InvalidCheck;
            const line = sql[i + 7 .. eol];
            const end = std.mem.lastIndexOfScalar(u8, line, ')') orelse return error.InvalidCheck;
            _ = state.addConstraint(.check, line[0..end]);
            pos += eol;
        }

        // FOREIGN KEY
        // TODO: multi-col?
        for (try db.raw("SELECT \"from\" cols, \"table\" ref_table, \"to\" ref_cols, on_update, on_delete FROM pragma_foreign_key_list(?)", table).fetchAll(Fk)) |fk| {
            _ = state.addConstraint(.foreign_key, fk);
        }

        return .{
            .db = db,
            .table = table,
            .changes = changes,
            .state = state,
        };
    }

    pub fn exec(self: *TwelveStep) !void {
        // 1-2. Disable foreign_keys, start tx
        // (to be handled by caller)

        // 3. Remember indexes, triggers, and views
        const objs_to_restore = try self.db.raw("SELECT sql FROM sqlite_schema WHERE tbl_name = ? AND type != 'table' AND name NOT LIKE 'sqlite_%'", self.table).pluck([]const u8);

        // We do 4-7 repeatedly, for each change. It's not very efficient, but it's easier to understand / implement correctly.
        for (self.changes) |ch| {
            try self.applyChange(ch);

            // 4. Create a new table with the desired schema
            try self.state.exec();

            // 5. Copy data from the old table to the new table
            try self.copyData(ch);

            // 6. Drop the old table
            try self.db.schema().dropTable(self.table);

            // 7. Rename the new table to old table's name
            try self.db.schema().renameTable(TEMP_TABLE, self.table);
        }

        // 8-9. Recreate indices (except auto), triggers, and views
        // (TODO: we might need to drop views first? Indices/triggers should be already gone at this point)
        for (objs_to_restore) |sql| {
            try self.db.conn.execAll(sql);
        }

        // 10. Run integrity checks
        try self.db.conn.execAll("PRAGMA foreign_key_check");
        try self.db.conn.execAll("PRAGMA integrity_check");

        // 11-12. Commit, re-enable foreign_keys
        // (to be handled by caller)
    }

    fn applyChange(self: *TwelveStep, change: TableChange) !void {
        switch (change) {
            .add_column => |args| {
                _ = self.state.column(args[0], args[1], args[2]);
            },
            .rename_column => |names| {
                const i = try self.findColumn(names[0]);
                self.state.columns.items[i].name = names[1];

                // TODO: constraints?
            },
            .drop_column => |name| {
                const i = try self.findColumn(name);
                _ = self.state.columns.orderedRemove(i);

                for (self.state.constraints.items, 0..) |constraint, j| {
                    if (constraint.refersOnly(name)) {
                        _ = self.state.constraints.orderedRemove(j);
                    }
                }
            },
            .add_constraint => |constraint| {
                _ = self.state.append("constraints", constraint);
            },
            .drop_constraint => |drop| {
                var i: usize = 0;
                while (i < self.state.constraints.items.len) {
                    const constraint = self.state.constraints.items[i];
                    const should_drop = switch (drop) {
                        .primary_key => constraint == .primary_key,
                        .unique => |cols| constraint == .unique and std.mem.eql(u8, constraint.unique, cols),
                        .check => |expr| constraint == .check and std.mem.eql(u8, constraint.check, expr),
                        .foreign_key => |cols| constraint == .foreign_key and std.mem.eql(u8, constraint.foreign_key.cols, cols),
                    };
                    if (should_drop) {
                        _ = self.state.constraints.orderedRemove(i);
                    } else {
                        i += 1;
                    }
                }
            },
        }
    }

    fn findColumn(self: *TwelveStep, name: []const u8) !usize {
        for (self.state.columns.items, 0..) |col, i| {
            if (std.mem.eql(u8, col.name, name)) {
                return i;
            }
        }

        return error.ColumnNotFound;
    }

    fn copyData(self: *TwelveStep, change: TableChange) !void {
        var buf = try SqlBuf.init(self.db.arena);
        try buf.append("INSERT INTO ");
        try buf.appendIdent(self.state.table);
        try buf.append(" (");

        var i: usize = 0;
        for (self.state.columns.items) |col| {
            if (change == .add_column and std.mem.eql(u8, col.name, change.add_column[0])) {
                continue;
            }

            if (i > 0) try buf.append(", ");
            try buf.appendIdent(col.name);
            i += 1;
        }

        try buf.append(") SELECT ");

        var j: usize = 0;
        for (self.state.columns.items) |col| {
            if (change == .add_column and std.mem.eql(u8, col.name, change.add_column[0])) {
                continue;
            }

            const col_name = if (change == .rename_column and std.mem.eql(u8, col.name, change.rename_column[1]))
                change.rename_column[0]
            else
                col.name;

            if (j > 0) try buf.append(", ");
            try buf.appendIdent(col_name);
            j += 1;
        }

        try buf.append(" FROM ");
        try buf.appendIdent(self.table);

        try self.db.conn.execAll(buf.buf.items);
    }
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
        \\CREATE TABLE "person" (
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
        \\CREATE TABLE "person" (
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
        \\CREATE TABLE "employee" (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  department_id INTEGER NOT NULL,
        \\  PRIMARY KEY (id),
        \\  FOREIGN KEY (department_id) REFERENCES department (id)
        \\) STRICT
    );

    try expectDdl(&db, "department",
        \\CREATE TABLE "department" (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  PRIMARY KEY (id)
        \\) STRICT
    );

    try schema.dropTable("department");
    try schema.dropTable("employee");
}

test "advanced alter" {
    var db = try createDb("");
    defer db.deinit();
    const schema = db.schema();

    // Create initial table
    try schema.createTable("employee")
        .id()
        .column("name", .text, .{})
        .exec();

    // Add more columns
    try schema.alterTable("employee")
        .addColumn("department", .text, .{})
        .addColumn("salary", .int, .{})
        .exec();

    // Few more changes
    try schema.alterTable("employee")
        .addConstraint(.unique, "name")
        .renameColumn("department", "team")
        .addCheck("salary > 0")
        .exec();

    // Check that it worked
    try expectDdl(&db, "employee",
        \\CREATE TABLE "employee" (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  team TEXT NOT NULL,
        \\  salary INTEGER NOT NULL,
        \\  PRIMARY KEY (id),
        \\  UNIQUE (name),
        \\  CHECK (salary > 0)
        \\) STRICT
    );

    // Now, let's extract department into its own table
    try schema.createTable("department")
        .id()
        .column("name", .text, .{})
        .exec();

    // Add a foreign key
    try schema.alterTable("employee")
        .dropColumn("team")
        .addColumn("department_id", .int, .{ .nullable = true })
        .addForeignKey("department_id", "department", .{})
        .exec();

    // And check again
    try expectDdl(&db, "department",
        \\CREATE TABLE "department" (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  PRIMARY KEY (id)
        \\) STRICT
    );
    try expectDdl(&db, "employee",
        \\CREATE TABLE "employee" (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  salary INTEGER NOT NULL,
        \\  department_id INTEGER,
        \\  PRIMARY KEY (id),
        \\  UNIQUE (name),
        \\  CHECK (salary > 0),
        \\  FOREIGN KEY (department_id) REFERENCES department (id)
        \\) STRICT
    );

    // Add extra check
    try schema.alterTable("employee")
        .addCheck("salary < 100000")
        .exec();

    // Check again
    try expectDdl(&db, "employee",
        \\CREATE TABLE "employee" (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  salary INTEGER NOT NULL,
        \\  department_id INTEGER,
        \\  PRIMARY KEY (id),
        \\  UNIQUE (name),
        \\  CHECK (salary > 0),
        \\  CHECK (salary < 100000),
        \\  FOREIGN KEY (department_id) REFERENCES department (id)
        \\) STRICT
    );
}

test "data migration" {
    var db = try createDb("");
    defer db.deinit();
    const schema = db.schema();

    // Create initial table with data
    try schema.createTable("contacts")
        .id()
        .column("name", .text, .{})
        .column("phone", .text, .{})
        .exec();

    // Insert some test data
    try db.raw("INSERT INTO contacts (name, phone) VALUES (?, ?)", .{ "Alice", "555-1234" }).exec();
    try db.raw("INSERT INTO contacts (name, phone) VALUES (?, ?)", .{ "Bob", "555-5678" }).exec();

    // Perform schema changes
    try schema.alterTable("contacts")
        .addColumn("email", .text, .{ .nullable = true })
        .renameColumn("phone", "phone_number")
        .exec();

    // Verify the schema changes
    try expectDdl(&db, "contacts",
        \\CREATE TABLE "contacts" (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  "phone_number" TEXT NOT NULL, "email" TEXT,
        \\  PRIMARY KEY (id)
        \\) STRICT
    );

    // Verify the data was preserved
    const alice = try db.raw("SELECT * FROM contacts WHERE name = ?", "Alice").fetchOne(struct {
        id: u32,
        name: []const u8,
        phone_number: []const u8,
        email: ?[]const u8,
    });
    try t.expectEqualStrings("Alice", alice.?.name);
    try t.expectEqualStrings("555-1234", alice.?.phone_number);
    try t.expectEqual(@as(?[]const u8, null), alice.?.email);
}

test "drop constraints" {
    var db = try createDb("");
    defer db.deinit();
    const schema = db.schema();

    // Create initial table
    try schema.createTable("employee")
        .id()
        .column("name", .text, .{ .unique = true })
        .column("age", .int, .{})
        .column("department_id", .int, .{})
        .check("age > 0")
        .foreignKey("department_id", "department", .{})
        .exec();

    try schema.createTable("department")
        .id()
        .column("name", .text, .{})
        .exec();

    // Check
    try expectDdl(&db, "employee",
        \\CREATE TABLE "employee" (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  age INTEGER NOT NULL,
        \\  department_id INTEGER NOT NULL,
        \\  PRIMARY KEY (id),
        \\  UNIQUE (name),
        \\  CHECK (age > 0),
        \\  FOREIGN KEY (department_id) REFERENCES department (id)
        \\) STRICT
    );

    // Drop UNIQUE
    try schema.alterTable("employee")
        .dropUnique("name")
        .exec();

    // Check
    try expectDdl(&db, "employee",
        \\CREATE TABLE "employee" (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  age INTEGER NOT NULL,
        \\  department_id INTEGER NOT NULL,
        \\  PRIMARY KEY (id),
        \\  CHECK (age > 0),
        \\  FOREIGN KEY (department_id) REFERENCES department (id)
        \\) STRICT
    );

    // Drop CHECK
    try schema.alterTable("employee")
        .dropCheck("age > 0")
        .exec();

    // Check
    try expectDdl(&db, "employee",
        \\CREATE TABLE "employee" (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  age INTEGER NOT NULL,
        \\  department_id INTEGER NOT NULL,
        \\  PRIMARY KEY (id),
        \\  FOREIGN KEY (department_id) REFERENCES department (id)
        \\) STRICT
    );

    // Drop FOREIGN KEY
    try schema.alterTable("employee")
        .dropForeignKey("department_id")
        .exec();

    // Check
    try expectDdl(&db, "employee",
        \\CREATE TABLE "employee" (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  age INTEGER NOT NULL,
        \\  department_id INTEGER NOT NULL,
        \\  PRIMARY KEY (id)
        \\) STRICT
    );

    // Drop PRIMARY KEY
    try schema.alterTable("employee")
        .dropPrimaryKey()
        .exec();

    // Check
    try expectDdl(&db, "employee",
        \\CREATE TABLE "employee" (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  age INTEGER NOT NULL,
        \\  department_id INTEGER NOT NULL
        \\) STRICT
    );
}

test "drop multiple constraints at once" {
    var db = try createDb("");
    defer db.deinit();
    const schema = db.schema();

    // Create initial table
    try schema.createTable("user")
        .id()
        .column("email", .text, .{ .unique = true })
        .column("age", .int, .{})
        .column("status", .text, .{})
        .check("age >= 18")
        .check("status IN ('active', 'inactive')")
        .unique("email,status")
        .exec();

    // Check
    try expectDdl(&db, "user",
        \\CREATE TABLE "user" (
        \\  id INTEGER,
        \\  email TEXT NOT NULL,
        \\  age INTEGER NOT NULL,
        \\  status TEXT NOT NULL,
        \\  PRIMARY KEY (id),
        \\  UNIQUE (email),
        \\  UNIQUE (email,status),
        \\  CHECK (age >= 18),
        \\  CHECK (status IN ('active', 'inactive'))
        \\) STRICT
    );

    // Drop multiple
    try schema.alterTable("user")
        .dropUnique("email")
        .dropCheck("age >= 18")
        .dropUnique("email,status")
        .exec();

    // Check
    try expectDdl(&db, "user",
        \\CREATE TABLE "user" (
        \\  id INTEGER,
        \\  email TEXT NOT NULL,
        \\  age INTEGER NOT NULL,
        \\  status TEXT NOT NULL,
        \\  PRIMARY KEY (id),
        \\  CHECK (status IN ('active', 'inactive'))
        \\) STRICT
    );
}

test "alterTable().needsTwelveStep()" {
    var db = try createDb("");
    defer db.deinit();
    const schema = db.schema();

    // Setup a base table
    try schema.createTable("test")
        .id()
        .column("name", .text, .{})
        .exec();

    var alter = schema.alterTable("test");

    alter = alter.addColumn("nullable", .text, .{ .nullable = true });
    try t.expect(!alter.needsTwelveStep());

    alter = alter.addColumn("with_default", .int, .{ .default = "0" });
    try t.expect(!alter.needsTwelveStep());

    alter = alter.renameColumn("name", "new_name");
    try t.expect(!alter.needsTwelveStep());

    // Anything else should trigger TwelveStep
    alter = alter.addColumn("req", .text, .{});
    try t.expect(alter.needsTwelveStep());

    // As they also should alone
    try t.expect(schema.alterTable("test").addColumn("req", .text, .{}).needsTwelveStep());
    try t.expect(schema.alterTable("test").addUnique("name").needsTwelveStep());
    try t.expect(schema.alterTable("test").dropColumn("name").needsTwelveStep());
    try t.expect(schema.alterTable("test").dropPrimaryKey().needsTwelveStep());
}
