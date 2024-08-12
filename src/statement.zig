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
        try self.vtable.bind(self.handle, index, try Value.from(arg, self.session.?.arena));
    }

    pub fn bindAll(self: *Statement, args: anytype) !void {
        inline for (0..args.len) |i| {
            try self.bind(i, args[i]);
        }
    }

    pub fn exec(self: *Statement) !void {
        while (try self.step()) {
            // SQLite needs this (should be harmless for others)
        }
    }

    pub fn value(self: *Statement, comptime V: type) !?V {
        return if (try self.row(struct { V })) |r| r[0] else null;
    }

    pub fn row(self: *Statement, comptime R: type) !?R {
        if (try self.next(R)) |r| {
            try self.reset();
            return r;
        }

        return null;
    }

    pub fn all(self: *Statement, comptime R: type) ![]const R {
        var res = std.ArrayList(R).init(self.session.?.arena);

        while (try self.next(R)) |r| {
            try res.append(r);
        }

        return res.toOwnedSlice();
    }

    pub fn next(self: *Statement, comptime R: type) !?R {
        if (!try self.step()) {
            return null;
        }

        var res: R = undefined;

        inline for (@typeInfo(@TypeOf(res)).Struct.fields, 0..) |f, i| {
            @field(res, f.name) = try self.column(f.type, i);
        }

        return res;
    }

    fn step(self: *Statement) !bool {
        return self.vtable.step(self.handle);
    }

    fn column(self: *Statement, comptime V: type, index: usize) !V {
        if (comptime util.isString(V)) {
            return (try self.vtable.column(self.handle, index, .string)).string;
        }

        return switch (@typeInfo(V)) {
            .Bool => (try self.vtable.column(self.handle, index, .bool)).bool,
            .Int, .ComptimeInt => @intCast((try self.vtable.column(self.handle, index, .int)).int),
            else => {
                @panic("TODO");
            },
        };
    }

    pub fn reset(self: *Statement) !void {
        try self.vtable.reset(self.handle);
    }
};
