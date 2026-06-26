const std = @import("std");
const util = @import("util.zig");
const Value = @import("value.zig").Value;
const Session = @import("session.zig").Session;
const Error = @import("error.zig").Error;

/// Low-level prepared statement - avoid using.
pub const Statement = extern struct {
    handle: *anyopaque,
    vtable: *const VTable(*anyopaque),

    pub fn VTable(comptime H: type) type {
        return struct {
            bind: *const fn (self: H, index: usize, arg: Value) Error!void,
            columnCount: *const fn (self: H) usize,
            columnName: *const fn (self: H, index: usize) []const u8,
            column: *const fn (self: H, index: usize) Error!Value,
            step: *const fn (self: H) Error!bool,
            reset: *const fn (self: H) Error!void,
            deinit: *const fn (self: H) void,
        };
    }

    pub fn deinit(self: *Statement) void {
        self.vtable.deinit(self.handle);
    }

    /// Executes the statement.
    pub fn exec(self: *Statement) !void {
        while (try self.step()) {
            // SQLite needs this (should be harmless for others)
        }
    }

    /// Bind a value to the given index
    pub fn bind(self: *Statement, index: usize, val: Value) !void {
        try self.vtable.bind(self.handle, index, val);
    }

    /// Get the number of columns in the result set
    pub fn columnCount(self: *Statement) usize {
        return self.vtable.columnCount(self.handle);
    }

    /// Get the name of the given column
    pub fn columnName(self: *Statement, index: usize) []const u8 {
        return self.vtable.columnName(self.handle, index);
    }

    /// Get the value of the given column
    pub fn column(self: *Statement, index: usize) !Value {
        return self.vtable.column(self.handle, index);
    }

    /// Step the result set
    pub fn step(self: *Statement) !bool {
        return self.vtable.step(self.handle);
    }

    /// Reset the statement to be executed again
    pub fn reset(self: *Statement) !void {
        try self.vtable.reset(self.handle);
    }
};
