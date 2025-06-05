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
        try buf.appendIdent(self.table);
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
        const sqlite = std.mem.eql(u8, self.db.conn.kind(), "sqlite3");
        const needs_twelvestep = true; // TODO

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

        const cols = try db
            .raw("SELECT name, lower(type) type, \"notnull\" not_null, dflt_value \"default\" FROM pragma_table_xinfo(?)", table)
            .fetchAll(Column);

        for (cols) |col| {
            const pk = (try db.raw("SELECT pk FROM pragma_table_xinfo(?) WHERE name = ?", .{ table, col.name }).get(bool)).?;

            _ = state.column(col.name, col.type, .{
                .nullable = !col.not_null or pk,
                .primary_key = pk,
                .unique = false, // TODO
            });
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

        // 8-9. Recreate indices, triggers, and views
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

                // TODO: constraints?
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
        defer buf.deinit();

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
        // .addConstraint(.unique, "name")
        .renameColumn("department", "team")
        // .addCheck("salary > 0")
        .exec();

    // Check that it worked
    try expectDdl(&db, "employee",
        \\CREATE TABLE "employee" (
        \\  id INTEGER,
        \\  name TEXT NOT NULL,
        \\  team TEXT NOT NULL,
        \\  salary INTEGER NOT NULL,
        \\  PRIMARY KEY (id)
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
        // .addForeignKey("department_id", "department", "id")
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
        \\  PRIMARY KEY (id)
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
        \\  phone_number TEXT NOT NULL,
        \\  email TEXT,
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
