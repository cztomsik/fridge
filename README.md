# Fridge

A small, batteries-included database library for Zig. It offers a type-safe
query builder, connection pooling, shorthands for common tasks, migrations, and
more.

## Features

- [x] Supports both bundling SQLite3 with your app or linking system SQLite3.
- [x] Type-safe query builder.
- [x] Connection pool.
- [x] Shortcuts for common tasks.
- [x] **Migrations** inspired by [David Röthlisberger](https://david.rothlis.net/declarative-schema-migration-for-sqlite/).
- [ ] Additional drivers (e.g., PostgreSQL).
- [ ] Documentation.

## Installation

To get started, get the library first:

```sh
zig fetch https://github.com/cztomsik/fridge/archive/refs/heads/main.tar.gz --save
```

Then, in your `build.zig`:

```zig
// Use .bundle = false if you want to link system SQLite3
const sqlite = b.dependency("fridge", .{ .bundle = true });
exe.root_module.addImport("fridge", sqlite.module("fridge"));
```

## Basic Usage

Fridge's API is highly generic and revolves around user-defined structs. Let's
start by adding a few imports and defining a simple struct for the `User` table:

```zig
const std = @import("std");
const fr = @import("fridge");

const User = struct {
    id: u32,
    name: []const u8,
    role: []const u8,
};
```

The primary API you'll interact with is always `Session`. This high-level API
wraps the connection with an arena allocator and provides a type-safe query
builder.

Sessions can be either one-shot or pooled. Let's start with the simplest case:

```zig
var db = try fr.Session.open(fr.SQLite3, allocator, .{ .filename = ":memory:" });
defer db.deinit();
```

As you can see, `Session.open()` is generic and expects a driver type,
allocator, and connection options. These connection options are driver-specific.

Currently, only SQLite3 is supported, but the API is designed to be easily
extendable to other drivers, including your own.

Now, let's do something useful with the session. For example, we can create a
table. Executing DDL statements is a bit special because it usually involves
multiple statements and doesn't return any rows. In such cases, you can access
`conn: Connection` directly and use low-level methods like `execAll()`,
`lastInsertRowId()`, etc.

```zig
try db.conn.execAll(
    \\CREATE TABLE User (
    \\  id INTEGER PRIMARY KEY,
    \\  name TEXT NOT NULL,
    \\  role TEXT NOT NULL
    \\);
)
```

Next, let's insert some data. Since this is a common operation, there's a
convenient shorthand:

```zig
try db.insert(User, .{
    .name = "Alice",
    .role = "admin",
});
```

Alternatively, you could also use the query builder directly:

```zig
try db.query(User).insert(.{
    .name = "Bob",
    .role = "user",
});
```

The difference here is subtle. For instance, you could add `onConflict()` before
calling `insert()`, or in the case of `update()`, you could add `where()`, which
is often more common.

Now, let's query the data back. The `query()` method returns a query builder
that, among other things, has a `findAll()` method.

```zig
for (try db.query(User).findAll()) |user| {
    std.log.debug("User: {}", .{user});
}
```

Of course, you can also use `where()` to filter the results:

```zig
for (try db.query(User).where(.role, "admin").findAll()) |user| {
    std.log.debug("Admin: {}", .{user});
}
```

Notably, the `.where()` method is type-safe and will only accept types compatible with the column type.

## Pooling

If you're building a web application, you might want to use a connection pool. Pooling improves performance and ensures that each user request gets its own session with separate transaction chains.

Here's how to use the `fr.Pool`:

```zig
// During your app initialization
var pool = fr.Pool.init(SQLite3, allocator, 5, .{ .filename = ":memory:" });
defer pool.deinit();

// Inside your request handler
var db = try pool.getSession(allocator); // per-request allocator
defer db.deinit(); // cleans up and returns the connection to the pool

// Now you can use the session as usual
_ = try db.query(User).findAll();
```

## Migrations

> **TODO: Currently, migrations only work with SQLite.**

Fridge includes a simple migration script that can be used with any DDL SQL
file. It expects a `CREATE XXX` statement for every table, view, trigger, etc.,
and will automatically create or drop the respective objects. The only
requirement is that all names must be quoted.

```sql
CREATE TABLE "User" (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
```

For tables, the script will try to reuse as much data as possible. It will first
create a new table with a temporary name, copy all data from the old table using
`INSERT INTO xxx ... FROM temp`, and finally drop the old table and rename the
new one.

This approach allows you to freely add or remove columns, though you can't
change their types or remove default values. While it's not a fully-fledged
migration system, it works surprisingly well for most cases.

```zig
try fr.migrate(allocator, "my.db", @embedFile("db_schema.sql"));
```

## License

MIT
