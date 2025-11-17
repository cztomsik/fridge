const builtin = @import("builtin");
const std = @import("std");
const util = @import("util.zig");
const Value = @import("value.zig").Value;
const Driver = @import("driver.zig").Driver;
const Dialect = @import("driver.zig").Dialect;
const Connection = @import("connection.zig").Connection;
const Statement = @import("statement.zig").Statement;

const c = @cImport(
    @cInclude("sqlite3.h"),
);

pub const SqliteDriver = struct {
    interface: Driver = .{
        .vtable = &.{
            .dialect = dialect,
            .execAll = execAll,
            .prepare = prepare,
            .rowsAffected = rowsAffected,
            .lastInsertRowId = lastInsertRowId,
            .lastError = lastError,
            .deinitConn = deinitConn,
            .bind = bind,
            .column = column,
            .step = step,
            .reset = reset,
            .deinitStmt = deinitStmt,
        },
    },

    pub const Options = struct {
        dir: ?[]const u8 = null,
        filename: [:0]const u8,
        flags: c_int = c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE | c.SQLITE_OPEN_FULLMUTEX | c.SQLITE_OPEN_EXRESCODE,
        busy_timeout: ?c_int = 5_000,
        foreign_keys: ?enum { off, on } = .on,
        extensions: []const []const u8 = &.{},
    };

    pub fn open(self: *SqliteDriver, allocator: std.mem.Allocator, options: Options) !Connection {
        if (options.dir) |dir| {
            if (std.mem.indexOfScalar(u8, options.filename, ':') == null) {
                std.fs.cwd().makePath(dir) catch {};
                const path = try std.fs.path.joinZ(allocator, &.{ dir, options.filename });
                defer allocator.free(path);

                return self.openPath(path, options);
            }
        }

        return self.openPath(options.filename, options);
    }

    fn openPath(self: *SqliteDriver, path: [:0]const u8, options: Options) !Connection {
        var pdb: ?*c.sqlite3 = null;
        errdefer _ = if (pdb != null) c.sqlite3_close(pdb); // SQLite may init the handle even if it fails to open the database.

        check(c.sqlite3_open_v2(path, &pdb, options.flags, null)) catch return error.ConnectionFailed;
        var db: Connection = .{ .driver = &self.interface, .handle = pdb.? };

        if (options.busy_timeout) |v| {
            _ = c.sqlite3_busy_timeout(pdb, v);
        }

        if (options.foreign_keys) |v| switch (v) {
            inline else => |t| db.execAll("PRAGMA foreign_keys = " ++ @tagName(t)) catch return error.ConnectionFailed,
        };

        for (options.extensions) |ext| {
            loadExtension(pdb.?, ext) catch return error.ConnectionFailed;
        }

        return db;
    }

    fn loadExtension(pdb: *c.sqlite3, name: []const u8) !void {
        var err: ?[*:0]const u8 = null;
        var buf: [255]u8 = undefined;
        const sym = try std.fmt.bufPrintZ(&buf, "sqlite3_{s}_init", .{name});

        switch (builtin.target.os.tag) {
            .macos, .linux => {
                const lib = dlopen(null, 1) orelse return error.LoadExtensionFailed;
                defer _ = std.c.dlclose(lib);

                if (std.c.dlsym(lib, sym)) |p| {
                    // NOTE: Statically linked extensions should be compiled with -DSQLITE_CORE,
                    //       in which case the sqlite3_api_routines parameter is unused.
                    //       see https://github.com/sqlite/sqlite/blob/eaa50b866075f4c1a19065600e4f1bae059eb505/src/sqlite3ext.h#L712
                    const init: *const fn (*c.sqlite3, *?[*:0]const u8, *const c.sqlite3_api_routines) callconv(.c) c_int = @ptrCast(@alignCast(p));
                    return check(init(pdb, &err, undefined));
                }
            },
            else => {}, // TODO: Windows
        }

        if (comptime @hasField(c, "sqlite3_enable_load_extension")) {
            const zName = try std.fmt.bufPrintZ(&buf, "{s}", .{name});
            try check(c.sqlite3_load_extension(pdb, zName, null, null));
        } else {
            util.log.err("SQLite extension loading is disabled", .{});
            return error.LoadExtensionFailed;
        }
    }

    fn dialect(_: *Driver) Dialect {
        return .sqlite3;
    }

    fn execAll(_: *Driver, conn: Connection, sql: []const u8) !void {
        // TODO: c_allocator is just a quickfix for this https://github.com/cztomsik/fridge/blob/62bf1daeccf6781b5846ec70042d2ebaf3fb8644/src/sqlite.zig#L50
        const csql = try std.heap.c_allocator.dupeZ(u8, sql);
        defer std.heap.c_allocator.free(csql);

        try check(c.sqlite3_exec(@ptrCast(conn.handle), csql, null, null, null));
    }

    fn prepare(driver: *Driver, conn: Connection, sql: []const u8, params: []const Value) !Statement {
        var raw: ?*c.sqlite3_stmt = null;
        try check(c.sqlite3_prepare_v2(@ptrCast(conn.handle), sql.ptr, @intCast(sql.len), &raw, null));

        var stmt: Statement = .{
            .driver = driver,
            .handle = raw.?,
        };

        try stmt.bindAll(params);
        return stmt;
    }

    fn rowsAffected(_: *Driver, conn: Connection) !usize {
        return @intCast(c.sqlite3_changes(@ptrCast(conn.handle)));
    }

    fn lastInsertRowId(_: *Driver, conn: Connection) !i64 {
        return c.sqlite3_last_insert_rowid(@ptrCast(conn.handle));
    }

    fn lastError(_: *Driver, conn: Connection) []const u8 {
        return std.mem.span(c.sqlite3_errmsg(@ptrCast(conn.handle)));
    }

    fn deinitConn(_: *Driver, conn: Connection) void {
        _ = c.sqlite3_close(@ptrCast(conn.handle));
    }

    fn bind(_: *Driver, stmt: Statement, index: usize, arg: Value) !void {
        const raw: *c.sqlite3_stmt = @ptrCast(stmt.handle);
        const i: c_int = @intCast(index + 1);

        try check(switch (arg) {
            .null => c.sqlite3_bind_null(raw, i),
            .int => |v| c.sqlite3_bind_int64(raw, i, v),
            .float => |v| c.sqlite3_bind_double(raw, i, v),
            .string => |v| c.sqlite3_bind_text(raw, i, v.ptr, @intCast(v.len), null),
            .blob => |v| c.sqlite3_bind_blob(raw, i, v.ptr, @intCast(v.len), null),
        });
    }

    fn column(_: *Driver, stmt: Statement, index: usize) !Value {
        const raw: *c.sqlite3_stmt = @ptrCast(stmt.handle);
        const i: c_int = @intCast(index);

        return switch (c.sqlite3_column_type(raw, i)) {
            c.SQLITE_NULL => .null,
            c.SQLITE_INTEGER => .{ .int = c.sqlite3_column_int64(raw, i) },
            c.SQLITE_FLOAT => .{ .float = c.sqlite3_column_double(raw, i) },
            c.SQLITE_TEXT => .{ .string = c.sqlite3_column_text(raw, i)[0..@intCast(c.sqlite3_column_bytes(raw, i))] },
            c.SQLITE_BLOB => .{ .blob = @as([*c]const u8, @ptrCast(c.sqlite3_column_blob(raw, i)))[0..@intCast(c.sqlite3_column_bytes(raw, i))] },
            else => @panic("Unexpected column type"), // TODO: return error
        };
    }

    fn step(_: *Driver, stmt: Statement) !bool {
        return switch (c.sqlite3_step(@ptrCast(stmt.handle))) {
            c.SQLITE_ROW => true,
            c.SQLITE_DONE => false,
            else => |code| {
                errdefer if (c.sqlite3_db_handle(@ptrCast(stmt.handle))) |db| {
                    util.log.debug("{s}", .{c.sqlite3_errmsg(db)});
                };

                try check(code);
                unreachable;
            },
        };
    }

    fn reset(_: *Driver, stmt: Statement) !void {
        try check(c.sqlite3_reset(@ptrCast(stmt.handle)));
    }

    fn deinitStmt(_: *Driver, stmt: Statement) void {
        _ = c.sqlite3_finalize(@ptrCast(stmt.handle));
    }
};

pub fn check(code: c_int) !void {
    errdefer {
        util.log.err("SQLite error: {} {s}", .{ code, c.sqlite3_errstr(code) });
    }

    return switch (code) {
        c.SQLITE_OK, c.SQLITE_DONE, c.SQLITE_ROW => {},
        c.SQLITE_CONSTRAINT_CHECK => error.CheckViolation,
        c.SQLITE_CONSTRAINT_FOREIGNKEY => error.ForeignKeyViolation,
        c.SQLITE_CONSTRAINT_NOTNULL => error.NotNullViolation,
        c.SQLITE_CONSTRAINT_PRIMARYKEY, c.SQLITE_CONSTRAINT_UNIQUE => error.UniqueViolation,
        else => error.DbError,
    };
}

// Because std.c.dlopen is wrong.
extern "c" fn dlopen(path: ?[*:0]const u8, mode: c_int) ?*anyopaque;
