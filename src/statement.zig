const std = @import("std");
const util = @import("util.zig");
const Value = @import("value.zig").Value;
const Session = @import("session.zig").Session;
const Error = @import("error.zig").Error;
const Driver = @import("driver.zig").Driver;

/// Low-level prepared statement - avoid using.
pub const Statement = struct {
    driver: *Driver,
    handle: *anyopaque,

    pub fn deinit(self: Statement) void {
        self.driver.vtable.deinitStmt(self.driver, self);
    }

    /// Executes the statement.
    pub fn exec(self: Statement) !void {
        while (try self.step()) {
            // SQLite needs this (should be harmless for others)
        }
    }

    /// Returns a single row from the current result set.
    /// NOTE that the database (ie. SQLite) is free to hold a lock while the
    /// result set is not completely exhausted!
    pub fn next(self: Statement, comptime R: type, arena: std.mem.Allocator) !?R {
        if (!try self.step()) {
            return null;
        }

        var res: R = undefined;

        switch (@typeInfo(@TypeOf(res))) {
            .array => |a| {
                inline for (0..a.len) |i| {
                    const val = try self.column(i);
                    res[i] = try val.into(a.child, arena);
                }
            },
            .@"struct" => |s| {
                inline for (s.fields, 0..) |f, i| {
                    const val = try self.column(i);
                    @field(res, f.name) = try val.into(f.type, arena);
                }
            },
            else => {
                const val = try self.column(0);
                res = try val.into(R, arena);
            },
        }

        return res;
    }

    /// Bind a value to the given index
    pub fn bind(self: Statement, index: usize, val: Value) !void {
        try self.driver.vtable.bind(self.driver, self, index, val);
    }

    /// Bind all values to the statement
    pub fn bindAll(self: Statement, args: []const Value) !void {
        for (args, 0..) |val, i| {
            try self.bind(i, val);
        }
    }

    /// Get the value of the given column
    pub fn column(self: Statement, index: usize) !Value {
        return self.driver.vtable.column(self.driver, self, index);
    }

    /// Step the result set
    pub fn step(self: Statement) !bool {
        return self.driver.vtable.step(self.driver, self);
    }

    /// Reset the statement to be executed again
    pub fn reset(self: Statement) !void {
        try self.driver.vtable.reset(self.driver, self);
    }
};
