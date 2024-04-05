const std = @import("std");
const log = std.log.scoped(.sqlite);

const c = @cImport(
    @cInclude("sqlite3.h"),
);

pub const migrate = @import("migrate.zig").migrate;

/// Convenience wrapper for a blob of data.
pub const Blob = struct { bytes: []const u8 };

/// Low-level SQLite database connection. It's main purpose is to provide a
/// way to prepare statements.
pub const SQLite3 = struct {
    db: *c.sqlite3,

    /// Opens a database connection in read/write mode, creating the file if it
    /// doesn't exist. The connection is safe to use from multiple threads and
    /// will serialize access to the database.
    pub fn open(filename: [*:0]const u8) !SQLite3 {
        const flags = c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE | c.SQLITE_OPEN_FULLMUTEX;

        var db: ?*c.sqlite3 = null;
        errdefer {
            // SQLite may init the handle even if it fails to open the database.
            if (db) |ptr| _ = c.sqlite3_close(ptr);
        }

        try check(c.sqlite3_open_v2(filename, &db, flags, null));

        return .{
            .db = db.?,
        };
    }

    /// Closes the database connection.
    pub fn close(self: *SQLite3) void {
        _ = c.sqlite3_close(self.db);
    }

    /// Sets the busy timeout in milliseconds. 0 means no timeout.
    pub fn setBusyTimeout(self: *SQLite3, ms: u32) !void {
        try check(c.sqlite3_busy_timeout(self.db, @intCast(ms)));
    }

    /// Executes all the SQL statements in the given string, ignoring any rows
    /// they return.
    pub fn execAll(self: *SQLite3, sql: []const u8) !void {
        var next = std.mem.trimRight(u8, sql, " \n\t");

        while (true) {
            var stmt = try self.prepare(next);
            defer stmt.deinit();

            try stmt.exec();
            next = stmt.tail orelse return;
        }
    }

    /// Creates a prepared statement from the given SQL.
    pub fn prepare(self: *SQLite3, sql: []const u8) !Statement {
        errdefer {
            log.debug("{s}", .{c.sqlite3_errmsg(self.db)});
            log.debug("Failed to prepare SQL: {s}\n", .{sql});
        }

        var stmt: ?*c.sqlite3_stmt = null;
        var tail: [*c]const u8 = null;
        try check(c.sqlite3_prepare_v2(self.db, sql.ptr, @intCast(sql.len), &stmt, &tail));

        return .{
            .stmt = stmt.?,
            .tail = if (tail != null and tail != sql.ptr + sql.len) sql[@intFromPtr(tail) - @intFromPtr(sql.ptr) ..] else null,
        };
    }

    /// Returns the row ID of the most recent successful INSERT into the
    /// database.
    pub fn lastInsertRowId(self: *SQLite3) !i64 {
        return c.sqlite3_last_insert_rowid(self.db);
    }

    /// Returns the number of rows affected by the last INSERT, UPDATE or
    /// DELETE statement.
    pub fn rowsAffected(self: *SQLite3) !usize {
        return @intCast(c.sqlite3_changes(self.db));
    }
};

/// A prepared statement. This is a low-level interface which keeps the SQLite
/// locked until the `deinit()` or `reset()` is called.
pub const Statement = struct {
    stmt: *c.sqlite3_stmt,
    tail: ?[]const u8,

    /// Deinitializes the prepared statement.
    pub fn deinit(self: *Statement) void {
        _ = c.sqlite3_finalize(self.stmt);
    }

    /// Binds the given argument to the prepared statement.
    pub fn bind(self: *Statement, index: usize, arg: anytype) !void {
        const i: c_int = @intCast(index + 1);

        try check(switch (@TypeOf(arg)) {
            @TypeOf(null) => c.sqlite3_bind_null(self.stmt, i),
            bool => c.sqlite3_bind_int(self.stmt, i, if (arg) 1 else 0),
            i32 => c.sqlite3_bind_int(self.stmt, i, arg),
            u32, i64, @TypeOf(1) => c.sqlite3_bind_int64(self.stmt, i, arg),
            u64 => c.sqlite3_bind_int64(self.stmt, i, @intCast(arg)),
            f64, @TypeOf(0.0) => c.sqlite3_bind_double(self.stmt, i, arg),
            []const u8, []u8, [:0]const u8, [:0]u8 => c.sqlite3_bind_text(self.stmt, i, arg.ptr, @intCast(arg.len), null),
            Blob => c.sqlite3_bind_blob(self.stmt, i, arg.bytes.ptr, @intCast(arg.bytes.len), null),
            else => |T| {
                const info = @typeInfo(T);

                if (comptime info == .Optional) {
                    return if (arg == null) check(c.sqlite3_bind_null(self.stmt, i)) else self.bind(index, arg.?);
                }

                if (comptime info == .Pointer and @typeInfo(info.Pointer.child) == .Array and @typeInfo(info.Pointer.child).Array.child == u8) {
                    return self.bind(index, @as([]const u8, arg));
                }

                @compileError("TODO " ++ @typeName(T));
            },
        });
    }

    /// Executes the prepared statement, ignoring any rows it returns.
    pub fn exec(self: *Statement) !void {
        while (try self.step() != .done) {}
    }

    /// Gets the value of the given column.
    pub fn column(self: *Statement, comptime T: type, index: usize) !T {
        const i: c_int = @intCast(index);

        return switch (T) {
            []const u8, [:0]const u8 => try self.column(?T, index) orelse error.NullPointer,
            ?[]const u8, ?[:0]const u8 => {
                const len = c.sqlite3_column_bytes(self.stmt, i);
                const data = c.sqlite3_column_text(self.stmt, i);

                return if (data != null) data[0..@intCast(len) :0] else null;
            },
            Blob => try self.column(?T, index) orelse error.NullPointer,
            ?Blob => {
                const len = c.sqlite3_column_bytes(self.stmt, i);
                const data: [*c]const u8 = @ptrCast(c.sqlite3_column_blob(self.stmt, i));

                return if (data != null) Blob{ .bytes = data[0..@intCast(len)] } else null;
            },
            else => switch (@typeInfo(T)) {
                .Bool => c.sqlite3_column_int(self.stmt, i) != 0,
                .Int => @intCast(c.sqlite3_column_int64(self.stmt, i)),
                .Float => @floatCast(c.sqlite3_column_double(self.stmt, i)),
                .Optional => |o| if (c.sqlite3_column_type(self.stmt, i) == c.SQLITE_NULL) null else try self.column(o.child, index),
                else => @compileError("TODO: " ++ @typeName(T)),
            },
        };
    }

    /// Advances the prepared statement to the next row.
    pub fn step(self: *Statement) !enum { row, done } {
        const code = c.sqlite3_step(self.stmt);

        return switch (code) {
            c.SQLITE_ROW => return .row,
            c.SQLITE_DONE => return .done,
            else => {
                errdefer if (c.sqlite3_db_handle(self.stmt)) |db| log.debug("{s}", .{c.sqlite3_errmsg(db)});

                try check(code);
                unreachable;
            },
        };
    }

    /// Resets the prepared statement, allowing it to be executed again.
    pub fn reset(self: *Statement) !void {
        try check(c.sqlite3_reset(self.stmt));
    }
};

pub fn check(code: c_int) !void {
    const SQLiteError = error{
        SQLITE_ABORT,
        SQLITE_AUTH,
        SQLITE_BUSY,
        SQLITE_CANTOPEN,
        SQLITE_CONSTRAINT,
        SQLITE_CORRUPT,
        SQLITE_DONE,
        SQLITE_EMPTY,
        SQLITE_ERROR,
        SQLITE_FORMAT,
        SQLITE_FULL,
        SQLITE_INTERNAL,
        SQLITE_INTERRUPT,
        SQLITE_IOERR,
        SQLITE_LOCKED,
        SQLITE_MISMATCH,
        SQLITE_MISUSE,
        SQLITE_NOLFS,
        SQLITE_NOMEM,
        SQLITE_NOTADB,
        SQLITE_NOTFOUND,
        SQLITE_NOTICE,
        SQLITE_OK,
        SQLITE_PERM,
        SQLITE_PROTOCOL,
        SQLITE_RANGE,
        SQLITE_READONLY,
        SQLITE_ROW,
        SQLITE_SCHEMA,
        SQLITE_TOOBIG,
        SQLITE_WARNING,
    };

    switch (code) {
        c.SQLITE_OK, c.SQLITE_DONE, c.SQLITE_ROW => return,
        else => {
            @setCold(true);

            log.err("SQLite error: {} {s}", .{ code, c.sqlite3_errstr(code) });

            inline for (comptime std.meta.fields(SQLiteError)) |f| {
                if (code == @field(c, f.name)) return @field(SQLiteError, f.name);
            }

            return error.SQLiteError;
        },
    }
}
