const std = @import("std");
const util = @import("util.zig");
const Value = @import("value.zig").Value;
const Session = @import("session.zig").Session;

pub const Statement = extern struct {
    session: ?*Session = null,
    handle: *anyopaque,
    vtable: *const VTable(*anyopaque),

    pub const Error = error{ DbError, OutOfMemory };

    pub fn VTable(comptime H: type) type {
        return struct {
            bind: *const fn (self: H, index: usize, arg: Value) Error!void,
            column: *const fn (self: H, index: usize, tag: std.meta.Tag(Value)) Error!Value,
            step: *const fn (self: H) Error!bool,
            reset: *const fn (self: H) Error!void,
            finalize: *const fn (self: H) Error!void,
        };
    }

    pub fn deinit(self: *Statement) void {
        self.vtable.finalize(self.handle) catch @panic("TODO");
    }

    pub fn bind(self: *Statement, index: usize, arg: anytype) !void {
        try self.bindValue(index, try Value.from(arg, self.session.?.arena));
    }

    pub fn bindAll(self: *Statement, args: anytype) !void {
        inline for (0..args.len) |i| {
            try self.bind(i, args[i]);
        }
    }

    pub fn bindValue(self: *Statement, index: usize, arg: Value) !void {
        try self.vtable.bind(self.handle, index, arg);
    }

    pub fn column(self: *Statement, comptime V: type, index: usize) !?V {
        return switch (@typeInfo(V)) {
            .Bool => (try self.vtable.column(self.handle, index, .bool)).bool,
            .Int, .ComptimeInt => @intCast((try self.vtable.column(self.handle, index, .int)).int),
            else => @panic("TODO"),
        };
    }

    pub fn exec(self: *Statement) !void {
        while (try self.step()) {
            // SQLite needs this (should be harmless for others)
        }
    }

    pub fn value(self: *Statement, comptime V: type) !?V {
        if (!try self.step()) {
            return null;
        }

        const res = try self.column(V, 0);
        try self.reset();
        return res;
    }

    pub fn row(self: *Statement, comptime R: type) !?R {
        defer self.reset();

        @panic("TODO");
        // return switch (try self.step()) {
        //     .row => self.readRow(R),
        //     .done => null,
        // };
    }

    pub fn all(self: *Statement, comptime R: type) ![]const R {
        _ = self;
        @panic("TODO");
        // var res = std.ArrayList(R).init(self.session.arena);

        // while (try self.step() == .row) {
        //     try res.append(
        //         try self.readRow(R),
        //     );
        // }

        // return res.toOwnedSlice();
    }

    fn step(self: *Statement) !bool {
        return self.vtable.step(self.handle);
    }

    fn reset(self: *Statement) !void {
        try self.vtable.reset(self.handle);
    }

    fn finalize(self: *Statement) !void {
        try self.vtable.finalize(self.handle);
    }
};
