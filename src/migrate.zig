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

    // Create empty database with the desired schema
    log.debug("-- Creating pristine database", .{});
    var pristine = try sqlite.SQLite3.open(":memory:");
    defer pristine.close();
    try pristine.execAll(ddl);

    // Start a transaction and disable foreign key checks
    try db.exec("BEGIN", .{});
    try db.exec("PRAGMA foreign_keys = OFF", .{});

    // Migrate each object type
    try migrateObjects(allocator, db, &pristine, "table");
    try migrateObjects(allocator, db, &pristine, "view");
    try migrateObjects(allocator, db, &pristine, "trigger");
    try migrateObjects(allocator, db, &pristine, "index");

    // Re-enable foreign key checks and commit
    try db.exec("PRAGMA foreign_keys = ON", .{});
    try db.exec("COMMIT", .{});
}

fn migrateObjects(allocator: std.mem.Allocator, db: *sqlite.SQLite3, pristine: *sqlite.SQLite3, kind: []const u8) !void {
    var objects = try pristine.query("SELECT name, sql FROM sqlite_master WHERE type = ? AND name != 'sqlite_sequence' AND name NOT LIKE 'sqlite_autoindex_%'", .{kind});
    defer objects.deinit();

    var it = objects.iterator(struct { []const u8, []const u8 });
    while (try it.next()) |row| {
        log.debug("-- Checking {s} {s}", .{ kind, row[0] });

        // Check if object exists
        if (!try db.get(bool, "SELECT COUNT(*) FROM sqlite_master WHERE type = ? AND name = ?", .{ kind, row[0] })) {
            log.debug("{s} does not exist, creating", .{kind});
            try db.exec(row[1], .{});
            continue;
        }

        // Check if object is the same
        if (try db.get(bool, "SELECT sql = ? FROM sqlite_master WHERE type = ? AND name = ?", .{ row[1], kind, row[0] })) {
            log.debug("{s} already exists and is the same", .{kind});
            continue;
        }

        if (std.mem.eql(u8, kind, "table")) {
            log.debug("{s} already exists but is different, migrating", .{kind});

            // First, create a temp table with the new schema
            const temp_sql = try std.fmt.allocPrint(allocator, "CREATE TABLE temp {s}", .{row[1][std.mem.indexOf(u8, row[1], "(").?..]});
            defer allocator.free(temp_sql);
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
        } else {
            log.debug("{s} already exists but is different, dropping and recreating", .{kind});

            // Drop old object
            const drop_sql = try std.fmt.allocPrint(allocator, "DROP {s} {s}", .{ kind, row[0] });
            defer allocator.free(drop_sql);
            try db.exec(drop_sql, .{});

            // Create new object
            try db.exec(row[1], .{});
        }
    }

    // Now we can check for extraneous objects and drop them

    const all_names = try db.getString(allocator, "SELECT json_group_array(name) FROM sqlite_master WHERE type = ?", .{kind});
    defer allocator.free(all_names);

    var extraneous = try pristine.query("SELECT json_each.value FROM json_each(?) WHERE json_each.value NOT IN (SELECT name FROM sqlite_master WHERE type = ?)", .{ all_names, kind });
    defer extraneous.deinit();

    var it2 = extraneous.iterator([]const u8);
    while (try it2.next()) |name| {
        log.debug("-- Dropping extraneous {s} {s}", .{ kind, name });

        const drop_sql = try std.fmt.allocPrint(allocator, "DROP {s} {s}", .{ kind, name });
        defer allocator.free(drop_sql);

        try db.exec(drop_sql, .{});
    }
}
