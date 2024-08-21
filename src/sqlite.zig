pub const std = @import("std");
const util = @import("util.zig");
const Value = @import("value.zig").Value;
const Connection = @import("connection.zig").Connection;
const Statement = @import("statement.zig").Statement;

const c = @cImport(
    @cInclude("sqlite3.h"),
);

pub const SQLite3 = opaque {
    pub const Options = struct {
        filename: [:0]const u8,
        flags: c_int = c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE | c.SQLITE_OPEN_FULLMUTEX | c.SQLITE_OPEN_EXRESCODE,
        busy_timeout: c_int = 0,
    };

    pub fn open(opts: Options) !*SQLite3 {
        var db: ?*c.sqlite3 = null;
        errdefer {
            // SQLite may init the handle even if it fails to open the database.
            if (db) |pdb| _ = c.sqlite3_close(pdb);
        }

        try check(c.sqlite3_open_v2(opts.filename.ptr, &db, opts.flags, null));
        try check(c.sqlite3_busy_timeout(db.?, opts.busy_timeout));
        return @ptrCast(db.?);
    }

    pub fn execAll(self: *SQLite3, sql: []const u8) !void {
        const csql = try std.heap.c_allocator.dupeZ(u8, sql);
        defer std.heap.c_allocator.free(csql);

        try check(c.sqlite3_exec(self.ptr(), csql, null, null, null));
    }

    pub fn prepare(self: *SQLite3, sql: []const u8) !Statement {
        var stmt: ?*c.sqlite3_stmt = null;
        try check(c.sqlite3_prepare_v2(self.ptr(), sql.ptr, @intCast(sql.len), &stmt, null));

        return util.upcast(@as(*Stmt, @ptrCast(stmt.?)), Statement);
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

    pub fn finalize(self: *Stmt) void {
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
