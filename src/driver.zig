const Value = @import("value.zig").Value;
const Error = @import("error.zig").Error;
const Connection = @import("connection.zig").Connection;
const Statement = @import("statement.zig").Statement;

pub const Dialect = enum { sqlite3, postgresql };

pub const Driver = struct {
    vtable: *const VTable,

    pub const VTable = struct {
        // Connection operations
        dialect: *const fn (driver: *Driver) Dialect,
        execAll: *const fn (driver: *Driver, conn: Connection, sql: []const u8) Error!void,
        prepare: *const fn (driver: *Driver, conn: Connection, sql: []const u8, params: []const Value) Error!Statement,
        rowsAffected: *const fn (driver: *Driver, conn: Connection) Error!usize,
        lastInsertRowId: *const fn (driver: *Driver, conn: Connection) Error!i64,
        lastError: *const fn (driver: *Driver, conn: Connection) []const u8,
        deinitConn: *const fn (driver: *Driver, conn: Connection) void,

        // Statement operations
        bind: *const fn (driver: *Driver, stmt: Statement, index: usize, arg: Value) Error!void,
        column: *const fn (driver: *Driver, stmt: Statement, index: usize) Error!Value,
        step: *const fn (driver: *Driver, stmt: Statement) Error!bool,
        reset: *const fn (driver: *Driver, stmt: Statement) Error!void,
        deinitStmt: *const fn (driver: *Driver, stmt: Statement) void,
    };
};
