const std = @import("std");
const util = @import("util.zig");
const Statement = @import("statement.zig").Statement;
const Driver = @import("driver.zig").Driver;
const Dialect = @import("driver.zig").Dialect;
const Value = @import("value.zig").Value;
const Error = @import("error.zig").Error;

pub const Connection = struct {
    handle: *anyopaque,
    driver: *Driver,

    /// Executes all SQL statements in the given string.
    pub fn execAll(self: Connection, sql: []const u8) Error!void {
        errdefer {
            util.log.debug("{s}", .{self.lastError()});
        }

        return self.driver.vtable.execAll(self.driver, self, sql);
    }

    /// Creates a prepared statement from the given SQL.
    pub fn prepare(self: Connection, sql: []const u8, params: []const Value) Error!Statement {
        errdefer {
            util.log.debug("{s}", .{self.lastError()});
            util.log.debug("Failed to prepare SQL: {s}\n", .{sql});
        }

        return self.driver.vtable.prepare(self.driver, self, sql, params);
    }

    /// Returns the number of rows modified by the last INSERT/UPDATE/DELETE.
    pub fn rowsAffected(self: Connection) Error!usize {
        return self.driver.vtable.rowsAffected(self.driver, self);
    }

    /// Returns the row ID of the last INSERT.
    pub fn lastInsertRowId(self: Connection) Error!i64 {
        return self.driver.vtable.lastInsertRowId(self.driver, self);
    }

    /// Returns the last error message.
    pub fn lastError(self: Connection) []const u8 {
        return self.driver.vtable.lastError(self.driver, self);
    }

    /// Closes the connection.
    pub fn deinit(self: Connection) void {
        self.driver.vtable.deinitConn(self.driver, self);
    }
};
