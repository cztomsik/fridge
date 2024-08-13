> Everything here is already outdated, I'm working on a new version which will
> be more pleasant to use.

# Fridge

A small, batteries-included database library for Zig. It provides a type-safe
query builder, connection pooling, shorthands for common tasks, migrations, and
more.

However, it's important to note that this is not and **will never be an ORM**.
It's designed to be a simple, low-abstraction, and efficient library for
interacting with SQLite databases (for now).

## Features

- [x] supports both bundling sqlite3 with your app or linking system sqlite3
- [x] type-safe query builder
- [x] connection pool
- [x] shorthands for common tasks
- [x] **migrations** inspired by [David Röthlisberger](https://david.rothlis.net/declarative-schema-migration-for-sqlite/)
- [ ] docs

## Installation

```sh
zig fetch https://github.com/cztomsik/fridge/archive/refs/heads/main.tar.gz --save
```

Then, in your `build.zig`:

```zig
// or .bundle = false if you want to link system sqlite3
const sqlite = b.dependency("fridge", .{ .bundle = true });
exe.root_module.addImport("fridge", sqlite.module("fridge"));
```

## Low-level API

The low-level API provides direct interaction with SQLite, but it's important to
note that using the `fr.Statement` can result in a `error.SQLITE_BUSY` if the same
table is modified concurrently. To avoid this, it is recommended to use the
higher-level `fr.Session` API. However, if you still prefer to use the low-level
API, here's an example for reference.

```zig
const fr = @import("fridge");

var conn = try fr.SQLite3.open(":memory:");
defer conn.close();

var stmt = try conn.prepare("SELECT 1", .{});
defer stmt.deinit();

try stmt.step(); // call this for every row

const one = try stmt.column(u32, 0); // get the first column as u32
```

## Session API and the query builder DSL

If you want to avoid locking tables, you can use the primary API, let's start
with the imports and a struct for the `User` table.

```zig
const std = @import("std");
const fr = @import("fridge");

const User = struct {
    id: u32,
    name: []const u8,
    role: []const u8,
};
```

Like in the low-level API, we need to open a connection to the database. To do
this, we need a `fr.Connection` and an arena `Allocator` so let's create them.

```zig
const conn = try fr.SQLite3.open(":memory:");

var arena = std.heap.ArenaAllocator.init(...);
defer arena.deinit();
```

Now, let's create the `Session` and some queries.

```zig
var session = fr.Session.fromConnection(arena.allocator(), conn);
defer session.deinit(); // will close the connection but not free any memory

const all_users = fr.query(User);
const admin_users = fr.query(User).where(.{ .role = "admin" });
```

Those queries are immutable and alloc-free, you can think about them as if they
were a DSL compiled to a static SQL string. It's not 100% true, but it's a good
mental model.

```zig
for (try session.findAll(admin_users)) |u| {
    std.log.debug("Admin user: {}", .{u});

    // Here, we could do any changes to the `User` table and we wouldn't
    // get the `error.SQLITE_BUSY` error.
    try session.update(User, u.id, .{ .role = "user" });
}
```

Interaction with the session may need to allocate and it might also fail, so we
need to use `try` here. Again, if you think about queries as a compiled DSL, we
are now executing the compiled query, which by itself is completely inert
(and useless).

The query itself is also completely independent of the session, you could use
it alone, with your own types, if you wanted to. You just need to provide a
`*std.ArrayList(u8)` for writing the SQL string and some `statement: anytype`
with a `stmt.bind(index, value)` method.

## Pooling

A connection pool is a collection of database connections that are created in
advance and can be reused by multiple clients. It helps improve performance and
scalability by reducing the overhead of establishing a new connection for each
client request.

Here's an example of how to use the `fr.Pool`:

```zig
var pool = fr.Pool.init(allocator, "my.db", 5);
defer pool.deinit();

// we still need arena for every session
var arena = ...
defer ...

// in this case, session.deinit() will return the connection to the pool
var session = try Session.fromPool(arena.allocator(), &pool);
defer session.deinit();

// now we can use the session as usual
_ = try session.findAll(fr.query(User));
```

## Migrations

> **TODO: This only works for SQLite**

There is a simple migration script which can be used with any DDL SQL file. It
expects a `CREATE XXX` statement for every table, view, trigger, etc. and it
will automatically create or drop respective objects. The only hard requirement
is that all names need to be quoted.

```sql
CREATE TABLE "User" (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
```

In the case of a table, it will try to reuse as much data as possible. It will
first create a new table with a temporary name, then it will copy all data from
the old table with `INSERT INTO xxx ... FROM temp` and finally it will drop the
old table and rename the new one.

This means you can freely add or remove columns, but you can't change their
types or remove default values. It's not fulfledged migration system, but it
works surprisingly well for most cases.

```zig
try fr.migrate(allocator, "my.db", @embedFile("db_schema.sql"));
```

## License

MIT
