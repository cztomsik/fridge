const std = @import("std");
const Connection = @import("connection.zig").Connection;
const Session = @import("session.zig").Session;

/// A simple connection pool. This is especially useful for web servers where
/// each request needs its own "session" with separate transactions. The pool
/// makes it easy to obtain a connection at the start of a request and release
/// it at the end.
pub const Pool = struct {
    conns: std.ArrayList(Connection),
    factory: *const fn (opts: *const anyopaque) Error!Connection,
    opts: *const anyopaque,
    max_count: usize,
    count: usize = 0,
    mutex: std.Thread.Mutex = .{},
    wait: std.Thread.Condition = .{},

    const Error = error{ DbError, OutOfMemory, PoolClosing };

    /// Initialize a connection pool with capacity for `max_count` connections
    /// which will be created using the provided driver-specific `options`.
    pub fn init(comptime T: type, allocator: std.mem.Allocator, max_count: usize, options: *const T.Options) Pool {
        const H = struct {
            fn open(opts: *const anyopaque) Error!Connection {
                return Connection.open(T, @as(*const T.Options, @ptrCast(@alignCast(opts))).*);
            }
        };

        return .{
            .conns = std.ArrayList(Connection).init(allocator),
            .opts = options,
            .factory = H.open,
            .max_count = max_count,
        };
    }

    pub fn getSession(self: *Pool, allocator: std.mem.Allocator) Error!Session {
        var sess = try Session.init(allocator, try self.getConnection());
        sess.pool = self;
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

        // TODO:
        // What's worse is that we might loose the connection (network error)
        // and then we should retry opening it. This is left for later.

        while (true) {
            if (self.conns.popOrNull()) |conn| {
                self.mutex.unlock();
                return conn;
            }

            if (self.count <= self.max_count) {
                const conn = try self.factory(self.opts);
                self.count += 1;

                self.mutex.unlock();
                return conn;
            }

            if (self.max_count == 0) {
                self.mutex.unlock();
                return error.PoolClosing;
            }

            self.wait.wait(&self.mutex);
            continue;
        }

        return self.conns[0];
    }

    /// Put the connection back into the pool and notify any waiting threads.
    pub fn releaseConnection(self: *Pool, conn: Connection) void {
        self.mutex.lock();
        self.conns.append(conn) catch unreachable;
        self.mutex.unlock();
        self.wait.signal();
    }

    /// Deinitialize the pool and close all connections.
    pub fn deinit(self: *Pool) void {
        self.mutex.lock();
        self.max_count = 0; // Make sure no new connections are created
        const count = self.count; // Save before unlocking
        self.mutex.unlock();

        // Reserve all connections and close them
        for (0..count) |_| {
            var conn = self.getConnection() catch unreachable;
            conn.close();
        }

        self.conns.deinit();
    }
};

const t = std.testing;

test {
    var pool = Pool.init(@import("sqlite.zig").SQLite3, t.allocator, 3, &.{
        .filename = ":memory:",
    });
    defer pool.deinit();

    const c1 = try pool.getConnection();
    try t.expectEqual(1, pool.count);
    try t.expectEqual(0, pool.conns.items.len);

    pool.releaseConnection(c1);
    try t.expectEqual(1, pool.count);
    try t.expectEqual(1, pool.conns.items.len);

    const c2 = try pool.getConnection();
    try t.expectEqual(1, pool.count);
    try t.expectEqual(0, pool.conns.items.len);
    try t.expectEqual(c1.handle, c2.handle);

    const c3 = try pool.getConnection();
    try t.expectEqual(2, pool.count);
    try t.expectEqual(0, pool.conns.items.len);
    try t.expect(c1.handle != c3.handle);

    pool.releaseConnection(c2);
    pool.releaseConnection(c3);
    try t.expectEqual(2, pool.count);
    try t.expectEqual(2, pool.conns.items.len);
}
