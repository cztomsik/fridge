// Inspired by https://david.rothlis.net/declarative-schema-migration-for-sqlite/
// What we do is a bit simpler though, we always recreate the table if it's
// different

const std = @import("std");
const dsl = @import("dsl.zig");
const SQLite3 = @import("sqlite.zig").SQLite3;
const Session = @import("session.zig").Session;
const log = std.log.scoped(.db_migrate);

pub fn migrate(allocator: std.mem.Allocator, filename: [*:0]const u8, ddl: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const mem = try SQLite3.open(":memory:");
    var pristine = Session.fromConnection(arena.allocator(), mem);
    defer pristine.deinit();

    const conn = try SQLite3.open(filename);
    var db = Session.fromConnection(arena.allocator(), conn);
    defer db.deinit();

    // Make sure we're in WAL mode and synchronous
    // (this is important for data integrity)
    try db.exec("PRAGMA journal_mode = WAL");
    try db.exec("PRAGMA synchronous = FULL");

    // Create empty database with the desired schema
    log.debug("-- Creating pristine database", .{});
    try pristine.conn.execAll(ddl);

    // Start a transaction and disable foreign key checks
    try db.exec("BEGIN");
    try db.exec("PRAGMA foreign_keys = OFF");

    // Migrate each object type
    inline for (.{ "table", "view", "trigger", "index" }) |kind| {
        try migrateObjects(&db, &pristine, kind);
    }

    // Re-enable foreign key checks and commit
    try db.exec("PRAGMA foreign_keys = ON");
    try db.exec("COMMIT");
}

fn migrateObjects(db: *Session, pristine: *Session, kind: []const u8) !void {
    const objects = dsl.query(sqlite_master)
        .where(.{ .type = kind })
        .where(dsl.raw("name NOT LIKE 'sqlite_%'", .{}));

    for (try pristine.findAll(objects)) |obj| {
        // Check if object exists
        const curr = try db.findBy(sqlite_master, .{ .type = kind, .name = obj.name }) orelse {
            logObject(obj, .create);
            try db.exec(obj.sql);
            continue;
        };

        // Check if object is the same
        if (std.mem.eql(u8, curr.sql, obj.sql)) {
            logObject(obj, .ok);
            continue;
        }

        if (std.mem.eql(u8, kind, "table")) {
            logObject(obj, .update);

            // First, create a temp table with the new schema
            const temp_sql = try std.fmt.allocPrint(db.arena, "CREATE TABLE temp {s}", .{obj.sql[std.mem.indexOf(u8, obj.sql, "(").?..]});
            try db.exec(temp_sql);

            // We want to copy data from the old table to the temp table so we need
            // to know which columns are common to both and we need to do it in a
            // new block so it gets deinitialized and we can then drop and rename
            // the temp table
            {
                var stmt = try db.prepare(dsl.raw("SELECT GROUP_CONCAT(name) FROM (SELECT name FROM pragma_table_xinfo(?) INTERSECT SELECT name FROM pragma_table_info('temp'))", .{obj.name}));
                defer stmt.deinit();

                _ = try stmt.step();
                const cols = try stmt.column([]const u8, 0);

                // Copy data from old table to temp table
                const copy_sql = try std.fmt.allocPrint(db.arena, "INSERT INTO temp({0s}) SELECT {0s} FROM {1s}", .{ cols, obj.name });
                try db.exec(copy_sql);
            }

            // Drop old table
            const drop_sql = try std.fmt.allocPrint(db.arena, "DROP TABLE {s}", .{obj.name});
            try db.exec(drop_sql);

            // Rename temp table to old table
            const rename_sql = try std.fmt.allocPrint(db.arena, "ALTER TABLE temp RENAME TO {s}", .{obj.name});
            try db.exec(rename_sql);
        } else {
            logObject(obj, .replace);

            // Drop old object
            const drop_sql = try std.fmt.allocPrint(db.arena, "DROP {s} {s}", .{ kind, obj.name });
            try db.exec(drop_sql);

            // Create new object
            try db.exec(obj.sql);
        }
    }

    // Now we can check for extraneous objects and drop them

    for (try db.findAll(objects)) |obj| {
        if (try pristine.findBy(sqlite_master, .{ .type = kind, .name = obj.name }) == null) {
            logObject(obj, .drop);

            const drop_sql = try std.fmt.allocPrint(db.arena, "DROP {s} {s}", .{ kind, obj.name });
            try db.exec(drop_sql);
        }
    }
}

fn logObject(obj: sqlite_master, status: enum { ok, create, update, replace, drop }) void {
    log.debug("{s:<7}  {s:<30}  {s}", .{ obj.type, obj.name, @tagName(status) });
}

const sqlite_master = struct {
    type: []const u8,
    name: []const u8,
    sql: []const u8,
};
