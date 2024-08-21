const std = @import("std");
const util = @import("util.zig");
const Statement = @import("statement.zig").Statement;
const Error = @import("error.zig").Error;

pub const Connection = struct {
    handle: *anyopaque,
    vtable: *const VTable(*anyopaque),

    pub fn VTable(comptime H: type) type {
        return struct {
            execAll: *const fn (self: H, sql: []const u8) Error!void,
            prepare: *const fn (self: H, sql: []const u8) Error!Statement,
            rowsAffected: *const fn (self: H) Error!usize,
            lastInsertRowId: *const fn (self: H) Error!i64,
            lastError: *const fn (self: H) []const u8,
            deinit: *const fn (self: H) void,
        };
    }

    pub fn open(comptime T: type, options: T.Options) !Connection {
        return util.upcast(try T.open(options), Connection);
    }

    /// Executes all SQL statements in the given string.
    pub fn execAll(self: Connection, sql: []const u8) Error!void {
        return self.vtable.execAll(self.handle, sql);
    }

    /// Creates a prepared statement from the given SQL.
    pub fn prepare(self: Connection, sql: []const u8) Error!Statement {
        errdefer {
            util.log.debug("{s}", .{self.lastError()});
            util.log.debug("Failed to prepare SQL: {s}\n", .{sql});
        }

        return self.vtable.prepare(self.handle, sql);
    }

    /// Returns the number of rows modified by the last INSERT/UPDATE/DELETE.
    pub fn rowsAffected(self: Connection) Error!usize {
        return self.vtable.rowsAffected(self.handle);
    }

    /// Returns the row ID of the last INSERT.
    pub fn lastInsertRowId(self: Connection) Error!i64 {
        return self.vtable.lastInsertRowId(self.handle);
    }

    /// Returns the last error message.
    pub fn lastError(self: Connection) []const u8 {
        return self.vtable.lastError(self.handle);
    }

    /// Closes the connection.
    pub fn deinit(self: Connection) void {
        self.vtable.deinit(self.handle);
    }
};
