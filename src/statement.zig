const std = @import("std");
const util = @import("util.zig");
const Value = @import("value.zig").Value;
const Session = @import("session.zig").Session;
const Error = @import("error.zig").Error;

pub const Statement = extern struct {
    handle: *anyopaque,
    vtable: *const VTable(*anyopaque),

    pub fn VTable(comptime H: type) type {
        return struct {
            bind: *const fn (self: H, index: usize, arg: Value) Error!void,
            column: *const fn (self: H, index: usize) Error!Value,
            step: *const fn (self: H) Error!bool,
            reset: *const fn (self: H) Error!void,
            deinit: *const fn (self: H) void,
        };
    }

    pub fn deinit(self: *Statement) void {
        self.vtable.deinit(self.handle);
    }

    pub fn exec(self: *Statement) !void {
        while (try self.step()) {
            // SQLite needs this (should be harmless for others)
        }
    }

    pub fn next(self: *Statement, comptime R: type, arena: std.mem.Allocator) !?R {
        if (!try self.step()) {
            return null;
        }

        var res: R = undefined;

        inline for (@typeInfo(@TypeOf(res)).Struct.fields, 0..) |f, i| {
            const val = try self.column(i);
            @field(res, f.name) = try val.into(f.type, arena);
        }

        return res;
    }

    pub fn bind(self: *Statement, index: usize, val: Value) !void {
        try self.vtable.bind(self.handle, index, val);
    }

    pub fn column(self: *Statement, index: usize) !Value {
        return self.vtable.column(self.handle, index);
    }

    pub fn step(self: *Statement) !bool {
        return self.vtable.step(self.handle);
    }

    pub fn reset(self: *Statement) !void {
        try self.vtable.reset(self.handle);
    }
};
