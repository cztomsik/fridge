const std = @import("std");
const log = std.log.scoped(.sqlite);

const c = @cImport(
    @cInclude("sqlite3.h"),
);

pub const migrate = @import("migrate.zig").migrate;

/// A SQLite database connection.
pub const SQLite3 = struct {
    db: *c.sqlite3,

    /// Opens a database connection in read/write mode, creating the file if it
    /// doesn't exist. The connection is safe to use from multiple threads and
    /// will serialize access to the database.
    pub fn open(filename: [*:0]const u8) !SQLite3 {
        const flags = c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE | c.SQLITE_OPEN_FULLMUTEX;

        var db: ?*c.sqlite3 = null;
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

    /// Shorthand for inserting a row into the given table. The row must be a
    /// struct with fields matching the columns of the table.
    pub fn insert(self: *SQLite3, comptime table_name: []const u8, row: anytype) !void {
        comptime var fields: []const u8 = "";
        comptime var placeholders: []const u8 = "";

        inline for (std.meta.fields(@TypeOf(row)), 0..) |f, i| {
            if (i > 0) {
                fields = fields ++ ", ";
                placeholders = placeholders ++ ", ";
            }

            fields = fields ++ f.name;
            placeholders = placeholders ++ "?";
        }

        try self.exec("INSERT INTO " ++ table_name ++ "(" ++ fields ++ ") VALUES (" ++ placeholders ++ ")", row);
    }

    /// Shorthand for updating a row in the given table. The row must be a
    /// struct with fields matching the columns of the table. The `id` field
    /// must be set to the ID of the row to update.
    pub fn update(self: *SQLite3, comptime table_name: []const u8, row: anytype) !void {
        comptime var fields: []const u8 = "";

        inline for (std.meta.fields(@TypeOf(row))) |f| {
            if (std.mem.eql(u8, f.name, "id")) continue;
            if (fields.len > 0) fields = fields ++ ", ";
            fields = fields ++ f.name ++ " = ?";
        }

        try self.exec("UPDATE " ++ table_name ++ " SET " ++ fields ++ " WHERE id = ?", row.id);
    }

    /// Executes the given SQL, ignoring any rows it returns.
    pub fn exec(self: *SQLite3, sql: []const u8, args: anytype) !void {
        var stmt = try self.query(sql, args);
        defer stmt.deinit();

        try stmt.exec();
    }

    pub fn execAll(self: *SQLite3, sql: []const u8) !void {
        var next = std.mem.trimRight(u8, sql, " \n\t");

        while (true) {
            var stmt = try self.query(next, .{});
            defer stmt.deinit();

            try stmt.exec();
            next = stmt.tail orelse return;
        }
    }

    /// Returns the number of rows affected by the last INSERT, UPDATE or
    /// DELETE statement.
    pub fn rowsAffected(self: *SQLite3) !usize {
        return @intCast(c.sqlite3_changes(self.db));
    }

    /// Shorthand for `self.query(sql, args).read(T)` where `T` is a primitive
    /// type. Returns the first value of the first row returned by the query.
    pub fn get(self: *SQLite3, comptime T: type, sql: []const u8, args: anytype) !T {
        var stmt = try self.query(sql, args);
        defer stmt.deinit();

        if (comptime !isPrimitive(T)) @compileError("Only primitive types are supported");

        return stmt.read(T);
    }

    /// Shorthand for `self.query(sql, args).read([]const u8)`. Returns the
    /// first column of the first row returned by the query. The returned slice
    /// needs to be freed by the caller.
    pub fn getString(self: *SQLite3, allocator: std.mem.Allocator, sql: []const u8, args: anytype) ![]const u8 {
        var stmt = try self.query(sql, args);
        defer stmt.deinit();

        return allocator.dupe(u8, try stmt.read([]const u8));
    }

    /// Shorthand for `self.prepare(sql).bindAll(args)`. Returns the prepared
    /// statement which still needs to be executed (and deinitialized).
    pub fn query(self: *SQLite3, sql: []const u8, args: anytype) !Statement {
        var stmt = try self.prepare(sql);
        try stmt.bindAll(args);
        return stmt;
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
            .sql = sql,
            .tail = if (tail != null and tail != sql.ptr + sql.len) sql[@intFromPtr(tail) - @intFromPtr(sql.ptr) ..] else null,
        };
    }
};

/// A prepared statement.
pub const Statement = struct {
    stmt: *c.sqlite3_stmt,
    sql: []const u8,
    tail: ?[]const u8,

    /// Deinitializes the prepared statement.
    pub fn deinit(self: *Statement) void {
        _ = c.sqlite3_finalize(self.stmt);
    }

    /// Binds the given argument to the prepared statement.
    pub fn bind(self: *Statement, index: usize, arg: anytype) !void {
        const i: c_int = @intCast(index + 1);

        try check(switch (@TypeOf(arg)) {
            bool => c.sqlite3_bind_int(self.stmt, i, if (arg) 1 else 0),
            i32 => c.sqlite3_bind_int(self.stmt, i, arg),
            u32, i64, @TypeOf(1) => c.sqlite3_bind_int64(self.stmt, i, arg),
            f64, @TypeOf(0.0) => c.sqlite3_bind_double(self.stmt, i, arg),
            []const u8, [:0]const u8 => c.sqlite3_bind_text(self.stmt, i, arg.ptr, @intCast(arg.len), null),
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

    /// Binds the given arguments to the prepared statement.
    /// Works with both structs and tuples.
    pub fn bindAll(self: *Statement, args: anytype) !void {
        inline for (std.meta.fields(@TypeOf(args)), 0..) |f, i| {
            try self.bind(i, @field(args, f.name));
        }
    }

    /// Executes the prepared statement, ignoring any rows it returns.
    pub fn exec(self: *Statement) !void {
        while (try self.step() != .done) {}
    }

    /// Reads the next row, either into a struct/tuple or a single value from
    /// the first column. Returns `error.NoRows` if there are no more rows.
    pub fn read(self: *Statement, comptime T: type) !T {
        return try self.readNext(T) orelse error.NoRows;
    }

    /// Reads the next row, either into a struct/tuple or a single value from
    /// the first column. Returns `null` if there are no more rows.
    pub fn readNext(self: *Statement, comptime T: type) !?T {
        if (try self.step() != .row) return null;

        if (comptime @typeInfo(T) == .Struct) {
            var res: T = undefined;

            inline for (std.meta.fields(T), 0..) |f, i| {
                @field(res, f.name) = try self.column(f.type, i);
            }

            return res;
        }

        return try self.column(T, 0);
    }

    /// Returns an iterator over the rows returned by the prepared statement.
    /// Only useful if you need iterator with argless `next()` and fixed return
    /// type.
    pub fn iterator(self: *Statement, comptime T: type) RowIterator(T) {
        return .{
            .stmt = self,
        };
    }

    /// Gets the value of the given column.
    pub fn column(self: *Statement, comptime T: type, index: usize) !T {
        const i: c_int = @intCast(index);

        return switch (T) {
            []const u8 => try self.column(?[]const u8, index) orelse error.NullPointer,
            ?[]const u8 => {
                const len = c.sqlite3_column_bytes(self.stmt, i);
                const data = c.sqlite3_column_text(self.stmt, i);

                return if (data != null) data[0..@intCast(len)] else null;
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

/// A generic iterator over the rows returned by a prepared statement.
pub fn RowIterator(comptime T: type) type {
    return struct {
        stmt: *Statement,

        pub fn next(self: *@This()) !?T {
            return self.stmt.readNext(T);
        }
    };
}

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

fn isPrimitive(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .Int, .Float, .Bool => true,
        .Optional => |o| isPrimitive(o.child),
        else => false,
    };
}
