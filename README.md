# ava-sqlite

Small wrapper around [sqlite3](https://sqlite.org/) extracted from
[ava](https://github.com/cztomsik/ava)

## Features

- [x] prepared statements
- [x] reading primitive types
- [x] reading strings
- [x] reading rows into structs
- [x] alloc-free iteration over rows
- [x] **migrations** inspired by [David Röthlisberger](https://david.rothlis.net/declarative-schema-migration-for-sqlite/)
- [ ] docs

## Installation

```sh
zig fetch https://github.com/cztomsik/ava-sqlite/archive/refs/heads/main.tar.gz --save
```

Then, in your `build.zig`:

```zig
exe.addModule("ava-sqlite", b.dependency("ava-sqlite", .{}).module("ava-sqlite"));
exe.linkSystemLibrary("sqlite3");
```

## Usage

```zig
const std = @import("std");
const sqlite = @import("ava-sqlite");

pub fn main() !void {
    var db = try sqlite.SQLite3.open(":memory:");
    var s = try db.query("SELECT 1", .{});
    defer s.deinit();

    std.log.debug("Hello {}", .{try s.read(u32)});
}
```

## Migrations

You can auto-migrate db with:

```zig
// Just make sure you have table names in quotes
try sqlite.migrate(allocator, &db, @embedFile("db_schema.sql"));
```

This will (re)create tables and reuse as much data as possible. It's not
fulfledged migration system, but it's good enough for most cases.

## License
MIT
