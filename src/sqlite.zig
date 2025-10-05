const builtin = @import("builtin");
const std = @import("std");
const util = @import("util.zig");
const Value = @import("value.zig").Value;
const Connection = @import("connection.zig").Connection;
const Statement = @import("statement.zig").Statement;

const c = @cImport(
    @cInclude("sqlite3.h"),
);

pub const SQLite3 = opaque {
    pub const Options = struct {
        dir: ?[]const u8 = null,
        filename: [:0]const u8,
        flags: c_int = c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE | c.SQLITE_OPEN_FULLMUTEX | c.SQLITE_OPEN_EXRESCODE,
        busy_timeout: ?c_int = 5_000,
        foreign_keys: ?enum { off, on } = .on,
        extensions: []const []const u8 = &.{},
    };

    pub fn open(allocator: std.mem.Allocator, options: Options) !*SQLite3 {
        if (options.dir) |dir| {
            if (std.mem.indexOfScalar(u8, options.filename, ':') == null) {
                std.fs.cwd().makePath(dir) catch {};
                const path = try std.fs.path.joinZ(allocator, &.{ dir, options.filename });
                defer allocator.free(path);

                return openPath(path, options);
            }
        }

        return openPath(options.filename, options);
    }

    fn openPath(path: [:0]const u8, options: Options) !*SQLite3 {
        var pdb: ?*c.sqlite3 = null;
        errdefer _ = if (pdb != null) c.sqlite3_close(pdb); // SQLite may init the handle even if it fails to open the database.

        check(c.sqlite3_open_v2(path, &pdb, options.flags, null)) catch return error.ConnectionFailed;
        const db: *SQLite3 = @ptrCast(pdb.?);

        if (options.busy_timeout) |v| {
            _ = c.sqlite3_busy_timeout(pdb, v);
        }

        if (options.foreign_keys) |v| switch (v) {
            inline else => |t| db.execAll("PRAGMA foreign_keys = " ++ @tagName(t)) catch return error.ConnectionFailed,
        };

        for (options.extensions) |ext| {
            db.loadExtension(ext) catch return error.ConnectionFailed;
        }

        return db;
    }

    pub fn loadExtension(self: *SQLite3, name: []const u8) !void {
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
                    return check(init(self.ptr(), &err, undefined));
                }
            },
            else => {}, // TODO: Windows
        }

        if (comptime @hasField(c, "sqlite3_enable_load_extension")) {
            const zName = try std.fmt.bufPrintZ(&buf, "{s}", .{name});
            try check(c.sqlite3_load_extension(self.ptr(), zName, null, null));
        } else {
            util.log.err("SQLite extension loading is disabled", .{});
            return error.LoadExtensionFailed;
        }
    }

    pub fn dialect(_: *SQLite3) Connection.Dialect {
        return .sqlite3;
    }

    pub fn execAll(self: *SQLite3, sql: []const u8) !void {
        // TODO: c_allocator is just a quickfix for this https://github.com/cztomsik/fridge/blob/62bf1daeccf6781b5846ec70042d2ebaf3fb8644/src/sqlite.zig#L50
        const csql = try std.heap.c_allocator.dupeZ(u8, sql);
        defer std.heap.c_allocator.free(csql);

        try check(c.sqlite3_exec(self.ptr(), csql, null, null, null));
    }

    pub fn prepare(self: *SQLite3, sql: []const u8, params: []const Value) !Statement {
        var raw_stmt: ?*c.sqlite3_stmt = null;
        try check(c.sqlite3_prepare_v2(self.ptr(), sql.ptr, @intCast(sql.len), &raw_stmt, null));

        var stmt = util.upcast(@as(*Stmt, @ptrCast(raw_stmt.?)), Statement);
        try stmt.bindAll(params);

        return stmt;
    }

    pub fn rowsAffected(self: *SQLite3) !usize {
        return @intCast(c.sqlite3_changes(self.ptr()));
    }

    pub fn lastInsertRowId(self: *SQLite3) !i64 {
        return c.sqlite3_last_insert_rowid(self.ptr());
    }

    pub fn lastError(self: *SQLite3) []const u8 {
        return std.mem.span(c.sqlite3_errmsg(self.ptr()));
    }

    pub fn deinit(self: *SQLite3) void {
        _ = c.sqlite3_close(self.ptr());
    }

    inline fn ptr(self: *SQLite3) *c.sqlite3 {
        return @ptrCast(self);
    }
};

const Stmt = opaque {
    pub fn bind(self: *Stmt, index: usize, arg: Value) !void {
        const i: c_int = @intCast(index + 1);

        try check(switch (arg) {
            .null => c.sqlite3_bind_null(self.ptr(), i),
            .int => |v| c.sqlite3_bind_int64(self.ptr(), i, v),
            .float => |v| c.sqlite3_bind_double(self.ptr(), i, v),
            .string => |v| c.sqlite3_bind_text(self.ptr(), i, v.ptr, @intCast(v.len), null),
            .blob => |v| c.sqlite3_bind_blob(self.ptr(), i, v.ptr, @intCast(v.len), null),
        });
    }

    pub fn column(self: *Stmt, index: usize) !Value {
        const i: c_int = @intCast(index);

        return switch (c.sqlite3_column_type(self.ptr(), i)) {
            c.SQLITE_NULL => .null,
            c.SQLITE_INTEGER => .{ .int = c.sqlite3_column_int64(self.ptr(), i) },
            c.SQLITE_FLOAT => .{ .float = c.sqlite3_column_double(self.ptr(), i) },
            c.SQLITE_TEXT => .{ .string = c.sqlite3_column_text(self.ptr(), i)[0..@intCast(c.sqlite3_column_bytes(self.ptr(), i))] },
            c.SQLITE_BLOB => .{ .blob = @as([*c]const u8, @ptrCast(c.sqlite3_column_blob(self.ptr(), i)))[0..@intCast(c.sqlite3_column_bytes(self.ptr(), i))] },
            else => @panic("Unexpected column type"), // TODO: return error
        };
    }

    pub fn step(self: *Stmt) !bool {
        return switch (c.sqlite3_step(self.ptr())) {
            c.SQLITE_ROW => true,
            c.SQLITE_DONE => false,
            else => |code| {
                errdefer if (c.sqlite3_db_handle(self.ptr())) |db| {
                    util.log.debug("{s}", .{c.sqlite3_errmsg(db)});
                };

                try check(code);
                unreachable;
            },
        };
    }

    pub fn reset(self: *Stmt) !void {
        try check(c.sqlite3_reset(self.ptr()));
    }

    pub fn deinit(self: *Stmt) void {
        _ = c.sqlite3_finalize(self.ptr());
    }

    inline fn ptr(self: *Stmt) *c.sqlite3_stmt {
        return @ptrCast(self);
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
