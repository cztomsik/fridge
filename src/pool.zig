const std = @import("std");
const sqlite = @import("sqlite.zig");

/// A simple connection pool. This is especially useful for web servers where
/// each request needs its own "session" with separate transactions. The pool
/// makes it easy to obtain a connection at the start of a request and release
/// it at the end.
pub const Pool = struct {
    allocator: std.mem.Allocator,
    conns: []sqlite.SQLite3,
    mutex: std.Thread.Mutex = .{},
    wait: std.Thread.Condition = .{},
    index: usize,

    /// Initialize a connection pool with `count` connections opened to the
    /// sqlite database at `filename`.
    pub fn init(allocator: std.mem.Allocator, filename: [*:0]const u8, count: usize) !Pool {
        const conns = try allocator.alloc(sqlite.SQLite3, count);
        errdefer allocator.free(conns);

        for (0..count) |i| {
            conns[i] = try sqlite.SQLite3.open(filename);

            // TODO: make this configurable
            try conns[i].setBusyTimeout(1_000);
        }

        return .{
            .allocator = allocator,
            .conns = conns,
            .index = count,
        };
    }

    /// Get a connection from the pool. If the pool is empty, this will block
    /// until a connection is available.
    pub fn get(self: *Pool) sqlite.SQLite3 {
        self.mutex.lock();

        while (true) {
            if (self.index > 0) {
                self.index -= 1;

                // Copy the connection out of the pool, after this point the
                // the original index may be overwritten.
                const conn = self.conns[self.index];
                self.mutex.unlock();
                return conn;
            }

            self.wait.wait(&self.mutex);
            continue;
        }

        return self.conns[0];
    }

    /// Put the connection back into the pool and notify any waiting threads.
    pub fn release(self: *Pool, conn: sqlite.SQLite3) void {
        self.mutex.lock();

        // Push to the "stack"
        self.conns[self.index] = conn;
        self.index += 1;

        self.mutex.unlock();
        self.wait.signal();
    }

    /// Deinitialize the pool and close all connections.
    pub fn deinit(self: *Pool) void {
        // Make sure nobody is using the pool & close all connections
        for (0..self.conns.len) |_| {
            var conn = self.get();
            conn.close();
        }

        self.allocator.free(self.conns);
    }
};
