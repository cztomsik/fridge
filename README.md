# ava-sqlite

Small wrapper around [sqlite3](https://sqlite.org/) extracted from
[ava](https://github.com/cztomsik/ava)

## Features

- [x] prepared statements
- [x] reading primitive types
- [x] reading strings
- [x] alloc-free iteration over rows
- [ ] migrations

## Installation

```sh
zig fetch https://github.com/cztomsik/ava-sqlite/archive/refs/heads/main.tar.gz --save
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

## License
MIT
