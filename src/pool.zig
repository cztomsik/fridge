const std = @import("std");
const util = @import("util.zig");
const Connection = @import("connection.zig").Connection;
const Statement = @import("statement.zig").Statement;
const Session = @import("session.zig").Session;

/// General options for the whole pool.
pub const PoolOptions = struct {
    max_count: usize = 10,
};

/// A simple connection pool. This is especially useful for web servers where
/// each request needs its own "session" with separate transactions. The pool
/// makes it easy to obtain a connection at the start of a request and release
/// it at the end.
///
/// NOTE: This struct is self-referential, so it needs to be heap-allocated or
/// pinned/never moved. Pinning is better, because the pool can still be
/// referenced when `deinit()` is being called - it will just fail.
pub fn Pool(comptime T: type) type {
    return struct {
        conns: std.array_list.Managed(PoolConnection(T)),
        conn_opts: T.Options,
        mutex: std.Thread.Mutex = .{},
        wait: std.Thread.Condition = .{},

        const Error = error{ OutOfMemory, ConnectionFailed, PoolClosing };

        /// Initialize a connection pool with capacity for `max_count` connections
        /// which will be created using the provided driver-specific `options`.
        pub fn init(allocator: std.mem.Allocator, pool_opts: PoolOptions, conn_opts: T.Options) !@This() {
            return .{
                .conns = try .initCapacity(allocator, pool_opts.max_count),
                .conn_opts = conn_opts,
            };
        }

        pub fn getSession(self: *@This(), allocator: std.mem.Allocator) Error!Session {
            return .init(allocator, try self.getConnection());
        }

        /// Get a connection from the pool. If the pool is empty, this will block
        /// until a connection is available.
        pub fn getConnection(self: *@This()) Error!Connection {
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
                    pconn.* = .{ .pool = self, .conn = try .open(T, self.conns.allocator, self.conn_opts) };
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
        pub fn deinit(self: *@This()) void {
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
}

fn PoolConnection(comptime T: type) type {
    return struct {
        pool: *Pool(T),
        conn: Connection,
        available: bool = false,

        const PoolConn = @This();

        inline fn check(res: anytype) @TypeOf(res) {
            return res catch |e| switch (e) {
                // TODO: check for error.BrokenPipe/ConnectionClosed and reconnect?
                else => e,
            };
        }

        pub fn dialect(self: *PoolConn) Connection.Dialect {
            return self.conn.dialect();
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
}

const t = std.testing;
const TestConn = @import("testing.zig").TestConn;

test Pool {
    var pool = try Pool(TestConn).init(t.allocator, .{ .max_count = 3 }, {});
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
    pool: *Pool(TestConn),
    thread: std.Thread,

    fn run(self: *Runner) !void {
        var count: usize = 0;
        while (self.pool.getConnection()) |conn| : (count += 1) {
            std.Thread.sleep(count % 10 * 10 * std.time.ns_per_ms);
            conn.deinit();
        } else |e| return switch (e) {
            error.PoolClosing => {},
            else => e,
        };
    }
};

test "Thread safety" {
    var pool = try Pool(TestConn).init(t.allocator, .{ .max_count = 3 }, {});
    defer pool.deinit();

    TestConn.created.store(0, .release);
    TestConn.destroyed.store(0, .release);

    var runners: [1000]Runner = undefined;
    for (&runners) |*r| r.* = .{
        .pool = &pool,
        .thread = try std.Thread.spawn(.{}, Runner.run, .{r}),
    };

    std.Thread.sleep(500 * std.time.ns_per_ms);
    pool.deinit();
    for (runners) |r| r.thread.join();

    try t.expectEqual(3, TestConn.created.load(.acquire));
    try t.expectEqual(3, TestConn.destroyed.load(.acquire));
}
