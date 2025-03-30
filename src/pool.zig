const std = @import("std");
const util = @import("util.zig");
const Connection = @import("connection.zig").Connection;
const Statement = @import("statement.zig").Statement;
const Session = @import("session.zig").Session;

/// A simple connection pool. This is especially useful for web servers where
/// each request needs its own "session" with separate transactions. The pool
/// makes it easy to obtain a connection at the start of a request and release
/// it at the end.
///
/// NOTE: This struct is self-referential, so it needs to be heap-allocated or
/// pinned/never moved. Pinning is better, because the pool can still be
/// referenced when `deinit()` is being called - it will just fail.
pub const Pool = struct {
    conns: std.ArrayList(PoolConn),
    factory: *const fn (opts: *const anyopaque) Error!Connection,
    opts: *const anyopaque,
    mutex: std.Thread.Mutex = .{},
    wait: std.Thread.Condition = .{},

    const Error = error{ OutOfMemory, ConnectionFailed, PoolClosing };

    /// Initialize a connection pool with capacity for `max_count` connections
    /// which will be created using the provided driver-specific `options`.
    pub fn init(comptime T: type, allocator: std.mem.Allocator, max_count: usize, options: *const T.Options) !Pool {
        const H = struct {
            fn open(opts: *const anyopaque) Error!Connection {
                return Connection.open(T, @as(*const T.Options, @ptrCast(@alignCast(opts))).*);
            }
        };

        return .{
            .conns = try std.ArrayList(PoolConn).initCapacity(allocator, max_count),
            .factory = H.open,
            .opts = options,
        };
    }

    pub fn getSession(self: *Pool, allocator: std.mem.Allocator) Error!Session {
        var sess = try Session.init(allocator, try self.getConnection());
        sess.owned = true;
        return sess;
    }

    /// Get a connection from the pool. If the pool is empty, this will block
    /// until a connection is available.
    pub fn getConnection(self: *Pool) Error!Connection {
        // TODO:
        // Opening a connection is driver-specific and it can take some time to
        // open it (e.g. DNS). It would be better to not block the entire pool
        // while waiting. On the other hand, it only happens once every time the
        // pool is empty, so maybe it's not worth the extra complexity.
        self.mutex.lock();
        defer self.mutex.unlock();

        // TODO:
        // What's worse is that we might loose the connection (network error)
        // and then we should retry opening it. This is left for later.

        while (true) {
            for (self.conns.items) |*pconn| {
                if (pconn.available) {
                    pconn.available = false;
                    return util.upcast(pconn, Connection);
                }
            }

            if (self.conns.items.len < self.conns.capacity) {
                const pconn = self.conns.addOneAssumeCapacity();
                pconn.* = .{ .pool = self, .conn = try self.factory(self.opts) };
                return util.upcast(pconn, Connection);
            }

            if (self.conns.capacity == 0) {
                return error.PoolClosing;
            }

            self.wait.wait(&self.mutex);
            continue;
        }
    }

    /// Deinitialize the pool and close all connections.
    pub fn deinit(self: *Pool) void {
        // Steal the list which will prevent any new connections (and deadlocks)
        var pconns = brk: {
            self.mutex.lock();
            defer self.mutex.unlock();
            break :brk self.conns.moveToUnmanaged();
        };

        // Notify all waiting threads that the pool is closing
        self.wait.broadcast();

        // Now we can aquire the mutex and continue without worying about deadlocks
        self.mutex.lock();
        defer self.mutex.unlock();

        // Gracefully close all connections
        for (pconns.items) |*pconn| {
            while (!pconn.available) {
                // Wait up to 5 seconds and then close the connection forcefully
                self.wait.timedWait(&self.mutex, 5 * std.time.ns_per_s) catch break;
            }

            pconn.conn.deinit();
        }

        pconns.deinit(self.conns.allocator);
    }
};

const PoolConn = struct {
    pool: *Pool,
    conn: Connection,
    available: bool = false,

    inline fn check(res: anytype) @TypeOf(res) {
        return res catch |e| switch (e) {
            // TODO: check for error.BrokenPipe/ConnectionClosed and reconnect?
            else => e,
        };
    }

    pub fn kind(self: *PoolConn) []const u8 {
        return self.conn.kind();
    }

    pub fn execAll(self: *PoolConn, sql: []const u8) !void {
        return check(self.conn.execAll(sql));
    }

    pub fn prepare(self: *PoolConn, sql: []const u8) !Statement {
        return check(self.conn.prepare(sql));
    }

    pub fn rowsAffected(self: *PoolConn) !usize {
        return check(self.conn.rowsAffected());
    }

    pub fn lastInsertRowId(self: *PoolConn) !i64 {
        return check(self.conn.lastInsertRowId());
    }

    pub fn lastError(self: *PoolConn) []const u8 {
        return self.conn.lastError();
    }

    pub fn deinit(self: *PoolConn) void {
        self.pool.mutex.lock();

        std.debug.assert(!self.available);
        self.available = true;

        self.pool.mutex.unlock();
        self.pool.wait.signal();
    }
};

const t = std.testing;

const TestConn = struct {
    id: u32,

    pub const Options = void;

    var created: std.atomic.Value(u32) = .{ .raw = 0 };
    var destroyed: std.atomic.Value(u32) = .{ .raw = 0 };

    pub fn open(_: void) !*TestConn {
        const ptr = try std.testing.allocator.create(TestConn);
        ptr.id = created.fetchAdd(1, .monotonic);
        return ptr;
    }

    pub fn kind(_: *TestConn) []const u8 {
        unreachable;
    }

    pub fn execAll(_: *TestConn, _: []const u8) !void {
        unreachable;
    }

    pub fn prepare(_: *TestConn, _: []const u8) !Statement {
        unreachable;
    }

    pub fn rowsAffected(_: *TestConn) !usize {
        unreachable;
    }

    pub fn lastInsertRowId(_: *TestConn) !i64 {
        unreachable;
    }

    pub fn lastError(_: *TestConn) []const u8 {
        unreachable;
    }

    pub fn deinit(self: *TestConn) void {
        std.testing.allocator.destroy(self);
        _ = destroyed.fetchAdd(1, .monotonic);
    }
};

test Pool {
    var pool = try Pool.init(TestConn, t.allocator, 3, &{});
    defer pool.deinit();

    var c1 = try pool.getConnection();
    try t.expectEqual(1, pool.conns.items.len);
    try t.expectEqual(false, pool.conns.items[0].available);

    c1.deinit();
    try t.expectEqual(1, pool.conns.items.len);
    try t.expectEqual(true, pool.conns.items[0].available);

    var c2 = try pool.getConnection();
    try t.expectEqual(1, pool.conns.items.len);
    try t.expectEqual(false, pool.conns.items[0].available);
    try t.expectEqual(c1.handle, c2.handle);

    var c3 = try pool.getConnection();
    try t.expectEqual(2, pool.conns.items.len);
    try t.expectEqual(false, pool.conns.items[0].available);
    try t.expectEqual(false, pool.conns.items[1].available);
    try t.expect(c1.handle != c3.handle);

    c2.deinit();
    c3.deinit();
    try t.expectEqual(2, pool.conns.items.len);
    try t.expectEqual(true, pool.conns.items[0].available);
    try t.expectEqual(true, pool.conns.items[1].available);
}

const Runner = struct {
    pool: *Pool,
    thread: std.Thread,

    fn run(self: *Runner) !void {
        var count: usize = 0;
        while (self.pool.getConnection()) |conn| : (count += 1) {
            std.time.sleep(count % 10 * 10 * std.time.ns_per_ms);
            conn.deinit();
        } else |e| return switch (e) {
            error.PoolClosing => {},
            else => e,
        };
    }
};

test "Thread safety" {
    var pool = try Pool.init(TestConn, t.allocator, 3, &{});
    defer pool.deinit();

    TestConn.created.store(0, .release);
    TestConn.destroyed.store(0, .release);

    var runners: [1000]Runner = undefined;
    for (&runners) |*r| r.* = .{
        .pool = &pool,
        .thread = try std.Thread.spawn(.{}, Runner.run, .{r}),
    };

    std.time.sleep(500 * std.time.ns_per_ms);
    pool.deinit();
    for (runners) |r| r.thread.join();

    try t.expectEqual(3, TestConn.created.load(.acquire));
    try t.expectEqual(3, TestConn.destroyed.load(.acquire));
}
