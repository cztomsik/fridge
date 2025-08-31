const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const bundle = b.option(bool, "bundle", "Bundle SQLite") orelse false;

    const lib = b.addModule("fridge", .{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib.link_libc = true;

    if (bundle) {
        const src = b.dependency("sqlite_source", .{});
        lib.addIncludePath(src.path("."));
        lib.addCSourceFile(.{ .file = src.path("sqlite3.c"), .flags = &.{"-std=c99"} });
    } else {
        // lib.linkSystemLibrary("sqlite3", .{});
        try lib.link_objects.append(b.allocator, .{
            .system_lib = .{
                .name = b.dupe("sqlite3"),
                .needed = false,
                .weak = false,
                .use_pkg_config = .yes,
                .preferred_link_mode = .dynamic,
                .search_strategy = .paths_first,
            },
        });
    }

    const test_filter = b.option([]const []const u8, "test-filter", "Skip tests that do not match any filter") orelse &[0][]const u8{};
    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    test_mod.link_objects = lib.link_objects;
    const tests = b.addTest(.{ .root_module = test_mod, .filters = test_filter });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_tests.step);
}
