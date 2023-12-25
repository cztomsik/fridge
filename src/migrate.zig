// Inspired by https://david.rothlis.net/declarative-schema-migration-for-sqlite/
// What we do is a bit simpler though, we always recreate the table if it's
// different

const std = @import("std");
const sqlite = @import("sqlite.zig");
const log = std.log.scoped(.db_migrate);

pub fn migrate(allocator: std.mem.Allocator, db: *sqlite.SQLite3, ddl: []const u8) !void {
    // Make sure we're in WAL mode and synchronous
    // (this is important for data integrity)
    try db.exec("PRAGMA journal_mode = WAL", .{});
    try db.exec("PRAGMA synchronous = FULL", .{});

    // Disable foreign key checks temporarily
    try db.exec("PRAGMA foreign_keys = OFF", .{});

    // Create empty database with the desired schema
    log.debug("-- Creating pristine database", .{});
    var pristine = try sqlite.SQLite3.open(":memory:");
    defer pristine.close();
    try pristine.execAll(ddl);

    // Get the list of table names and sql statements to create them
    var tables = try pristine.prepare("SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name != 'sqlite_sequence'");
    defer tables.deinit();

    // Go through that list and create/migrate tables as needed
    var it = tables.iterator(struct { []const u8, []const u8 });
    while (try it.next()) |row| {
        log.debug("-- Checking table {s}", .{row[0]});

        // Check if table exists
        if (!try db.get(bool, "SELECT COUNT(*) FROM sqlite_master WHERE name = ?", .{row[0]})) {
            log.debug("Table does not exist, creating", .{});
            try db.exec(row[1], .{});
            continue;
        }

        // Check if table is the same
        if (try db.get(bool, "SELECT sql = ? FROM sqlite_master WHERE name = ?", .{ row[1], row[0] })) {
            log.debug("Table already exists and is the same", .{});
            continue;
        }

        // Hold tight, we need to migrate the table
        log.debug("Table already exists but is different, migrating", .{});

        // First, create a temp table with the new schema
        const temp_sql = try std.fmt.allocPrint(allocator, "CREATE TABLE temp {s}", .{row[1][std.mem.indexOf(u8, row[1], "(").?..]});
        defer allocator.free(temp_sql);
        try db.exec("BEGIN", .{});
        try db.exec(temp_sql, .{});

        // We want to copy data from the old table to the temp table so we need
        // to know which columns are common to both and we need to do it in a
        // new block so it gets deinitialized and we can then drop and rename
        // the temp table
        {
            var cols = try db.query("SELECT GROUP_CONCAT(name) FROM (SELECT name FROM pragma_table_info(?) INTERSECT SELECT name FROM pragma_table_info('temp'))", .{row[0]});
            defer cols.deinit();

            // Copy data from old table to temp table
            const copy_sql = try std.fmt.allocPrint(allocator, "INSERT INTO temp({0s}) SELECT {0s} FROM {1s}", .{ try cols.read([]const u8), row[0] });
            defer allocator.free(copy_sql);
            try db.exec(copy_sql, .{});
        }

        // Drop old table
        log.debug("Dropping old table", .{});
        const drop_sql = try std.fmt.allocPrint(allocator, "DROP TABLE {s}", .{row[0]});
        defer allocator.free(drop_sql);
        try db.exec(drop_sql, .{});

        // Rename temp table to old table
        log.debug("Renaming temp table", .{});
        const rename_sql = try std.fmt.allocPrint(allocator, "ALTER TABLE temp RENAME TO {s}", .{row[0]});
        defer allocator.free(rename_sql);
        try db.exec(rename_sql, .{});

        // Commit transaction
        try db.exec("COMMIT", .{});
    }

    // Get the list of indices and sql statements to create them
    var indices = try pristine.prepare("SELECT name, sql FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%'");
    defer indices.deinit();

    // Go through that list and create/migrate tables as needed
    var it2 = indices.iterator(struct { []const u8, []const u8 });
    while (try it2.next()) |row| {
        log.debug("-- Checking index {s}", .{row[0]});

        // Check if index exists
        if (!try db.get(bool, "SELECT COUNT(*) FROM sqlite_master WHERE name = ?", .{row[0]})) {
            log.debug("Index does not exist, creating", .{});
            try db.exec(row[1], .{});
            continue;
        }

        // Check if table is the same
        if (try db.get(bool, "SELECT sql = ? FROM sqlite_master WHERE name = ?", .{ row[1], row[0] })) {
            log.debug("Index already exists and is the same", .{});
            continue;
        }

        log.debug("Index already exists but is different, migrating", .{});

        try db.exec("BEGIN", .{});

        // Drop old index
        const drop_sql = try std.fmt.allocPrint(allocator, "DROP INDEX {s}", .{row[0]});
        defer allocator.free(drop_sql);
        try db.exec(drop_sql, .{});

        // Create new index
        try db.exec(row[1], .{});
        try db.exec("COMMIT", .{});

        // TODO: delete old indices?
    }

    // Re-enable foreign key checks
    try db.exec("PRAGMA foreign_keys = ON", .{});
}
