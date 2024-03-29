const std = @import("std");
const builtin = @import("builtin");

const clap = @import("clap.zig");
const ecc = @import("ecc.zig");
const strings = @import("util/strings.zig");
const version = @import("version.zig");

const default_allocator = std.heap.page_allocator;

const Mode = enum { check, info, create, watch };

const CreationType = enum { count, datausage, coverage, dimension };
const CreationMode = struct {
    creationType: CreationType,
    int_val: u64 = 0,
    float_val: f64 = 0.0
};

const DirReference = struct {
    id: i32,
    name: []const u8
};

const IN_ACCESS = 0x00000001;
const IN_MODIFY = 0x00000002;
const IN_ATTRIB = 0x00000004;
const IN_CLOSE_WRITE = 0x00000008;
const IN_CLOSE_NOWRITE = 0x00000010;
const IN_CLOSE = (IN_CLOSE_WRITE | IN_CLOSE_NOWRITE);
const IN_OPEN = 0x00000020;
const IN_MOVED_FROM = 0x00000040;
const IN_MOVED_TO = 0x00000080;
const IN_MOVE = (IN_MOVED_FROM | IN_MOVED_TO);
const IN_CREATE = 0x00000100;
const IN_DELETE = 0x00000200;
const IN_DELETE_SELF = 0x00000400;
const IN_MOVE_SELF = 0x00000800;
const IN_ALL_EVENTS = 0x00000fff;

const IN_UNMOUNT = 0x00002000;
const IN_Q_OVERFLOW = 0x00004000;
const IN_IGNORED = 0x00008000;

const IN_ONLYDIR = 0x01000000;
const IN_DONT_FOLLOW = 0x02000000;
const IN_EXCL_UNLINK = 0x04000000;
const IN_MASK_CREATE = 0x10000000;
const IN_MASK_ADD = 0x20000000;

const IN_ISDIR = 0x40000000;
const IN_ONESHOT = 0x80000000;

const params = [_]clap.Param(clap.Help){
    clap.parseParam("-c, --check <FILE>     Checks a parity file for data file consistency. Cannot be combined with -imw.") catch unreachable,
    clap.parseParam("--dir <DIR>    Standard dir location for creating parity files in watch mode. Default is '.'.") catch unreachable,
    clap.parseParam("-f, --fix     Try to fix data errors found in data files based on parity files. Only works when checking a file. Default: false") catch unreachable,
    clap.parseParam("-i, --info <FILE>     Gets header statistics from a parity file. Cannot be combined with -cmw") catch unreachable,
    clap.parseParam("-m, --create <FILE>... Creates a parity file for specified data file. First argument is data file. Second optional argument is parity file location. Default is [datafilelocation].pars") catch unreachable,
    clap.parseParam("-r, --recursive    Iterates recursively through directory. Cannot be combined with -i") catch unreachable,
    clap.parseParam("-t, --target <FILE>    Specifies the target location file name for the parity file, either relative or absolute. Default is [data_file_name].pars") catch unreachable,
    clap.parseParam("-w, --watch <FILE>    Continuously watch a directory or file and constructs pars files on updates. Cannot be combined with -cim") catch unreachable,
    clap.parseParam("--blockcount <INT>    Fixes the number of blocks") catch unreachable,
    clap.parseParam("--datausage <FLOAT>    Fixes the size of the .pars file relative to the data file. 0<x<=1. Default 0.01") catch unreachable,
    clap.parseParam("--coverage  <INT>   Guarantee recovery every n bytes.") catch unreachable,
    clap.parseParam("--dimension <INT>    Fixes the dimension of the block. n >= 2") catch unreachable,
    clap.parseParam("-h, --help             Display this help and exit.") catch unreachable,
    clap.parseParam("-v, --version     Display the version number and exit.") catch unreachable,
};

pub fn main() !void {
    var iter = try clap.args.OsIterator.init(default_allocator);
    defer iter.deinit();

    var diag = clap.Diagnostic{};
    var args = clap.parse(clap.Help, &params, .{ .diagnostic = &diag }) catch |err| {
        diag.report(std.io.getStdOut().writer(), err) catch {};
        return;
    };
    defer args.deinit();

    if (args.flag("--help")) {
        try print_help();
        return;
    } else if (args.flag("--version")) {
        print("Pars version {d}.{d}.{d} © Léon van der Kaap 2024\nThis software is GPLv3 licensed.\n", .{ version.major, version.minor, version.patch });
        return;
    }

    const check_file = args.option("-c");
    const pars_info = args.option("-i");
    const create_parity_file = args.options("-m");
    const watch_mode = args.option("-w");
    const recursive = args.flag("-r") and builtin.os.tag == .linux;
    const fix = args.flag("-f");

    const block_count = args.option("--blockcount");
    const data_usage = args.option("--datausage");
    const coverage = args.option("--coverage");
    const dimension = args.option("--dimension");

    var creation_type: CreationType = .datausage;
    var int_val: u64 = 0;
    var float_val: f64 = 0.01;

    var creation_metric_count: u8 = 0;
    if (block_count != null) {
        creation_metric_count += 1;
        creation_type = .count;
    }
    if (data_usage != null) {
        creation_metric_count += 1;
        creation_type = .datausage;
    }
    if (coverage != null) {
        creation_metric_count += 1;
        creation_type = .coverage;
    }
    if (dimension != null) {
        creation_metric_count += 1;
        creation_type = .dimension;
    }

    if (creation_metric_count > 1) {
        print("Only one metric is allowed for choosing pars file size, {d} were chosen\n", .{creation_metric_count});
        std.os.exit(1);
    }

    var block_count_int = get_int_from_string(block_count) catch {
        print("Error converting string to int.\n", .{});
        std.os.exit(1);
    };
    if (block_count_int != null) {
        int_val = block_count_int.?;
    }

    var data_usage_float = get_float_from_string(data_usage) catch {
        print("Error converting string to float.\n", .{});
        std.os.exit(1);
    };
    if (data_usage_float != null) {
        float_val = data_usage_float.?;
    }

    var coverage_int: ?u64 = get_int_from_string(coverage) catch {
        print("Error converting string to int.\n", .{});
        std.os.exit(1);
    };
    if (coverage_int != null) {
        int_val = coverage_int.?;
    }

    var dimension_int: ?u64 = get_int_from_string(dimension) catch {
        print("Error converting string to int.\n", .{});
        std.os.exit(1);
    };
    if (dimension_int != null) {
        int_val = dimension_int.?;
    }

    var creationMode = CreationMode{
        .creationType = creation_type,
        .int_val = int_val,
        .float_val = float_val
    };

    var modes_selected: u8 = 0;
    if (check_file != null) modes_selected += 1;
    if (pars_info != null) modes_selected += 1;
    if (create_parity_file.len > 0) modes_selected += 1;
    if (watch_mode != null) modes_selected += 1;

    if (modes_selected != 1) {
        try print_help();
        return;
    }

    if (recursive and pars_info != null) {
        print("-r cannot be combined with -i\n. Exiting.", .{});
        std.os.exit(1);
    }

    var mode: Mode = undefined;

    if (check_file != null) {
        mode = .check;
    } else if (pars_info != null) {
        mode = .info;
    } else if (create_parity_file.len > 0) {
        mode = .create;
    } else if (watch_mode != null) {
        mode = .watch;
    } else {
        unreachable;
    }

    try execute_functionality(mode, check_file, pars_info, create_parity_file, watch_mode, fix, recursive, creationMode);
}

pub fn print(comptime format_string: []const u8, args: anytype) void {
    std.io.getStdOut().writer().print(format_string, args) catch return;
}

fn print_help() !void {
    print("Usage: pars [OPTION...] \nCreates parity files and attempts to restore corrupted data.\nIf arguments are possible, they are mandatory unless specified otherwise.\n\n", .{});
    var buf: [1024]u8 = undefined;
    var slice_stream = std.io.fixedBufferStream(&buf);
    try clap.help(std.io.getStdOut().writer(), &params);
    print("{s}\n", .{slice_stream.getWritten()});
}

fn execute_functionality(mode: Mode, check_file: ?[]const u8, pars_info: ?[]const u8, create_parity_file: []const []const u8, watch_mode: ?[]const u8, do_fix: bool, recursive: bool, creation_mode: CreationMode) !void {
    switch (mode) {
        .check => {
            if (recursive) {
                try recurse_check_files(check_file.?, do_fix);
            } else {
                try check_par_file(check_file.?, do_fix);
            }
        },
        .info => {
            try get_par_file_info(pars_info.?);
        },
        .create => {
            const data_location = create_parity_file[0];
            var relative_par_location: ?[]const u8 = null;
            if (create_parity_file.len > 1) {
                relative_par_location = create_parity_file[1];
            }

            if (recursive) {
                try recurse_create_files(data_location, creation_mode);
            } else {
                try do_create_parity_file(data_location, relative_par_location, creation_mode);
            }
        },
        .watch=> {
            if (builtin.os.tag == .linux) {
                try run_in_watch_mode(watch_mode.?, recursive, creation_mode);
            }
        },
    }
}

fn recurse_check_files(dir: []const u8, do_fix: bool) !void {
    var it_dir = std.fs.cwd().openIterableDir(dir, .{}) catch |err| {
        print("{any}", .{err});
        return;
    };
    defer it_dir.close();

    var walker = try it_dir.walk(default_allocator);
    defer walker.deinit();

    var buffer: [4096]u8 = undefined;
    var stringBuilder = strings.StringBuilder.init(buffer[0..]);

    while (try walker.next()) |entry| {
        concat_dir_and_subpath(&stringBuilder, dir, entry.path);

        if (std.mem.endsWith(u8, stringBuilder.toSlice(), ".pars")) {
            check_par_file(stringBuilder.toSlice(), do_fix) catch |err| {
                //TODO: Handle errors
                std.debug.print("{any}\n", .{err});
            };
        }
    }
}

fn recurse_create_files(dir: []const u8, creation_mode: CreationMode) !void {
    var it_dir = std.fs.cwd().openIterableDir(dir, .{}) catch |err| {
        print("{any}", .{err});
        return;
    };
    defer it_dir.close();

    var walker = try it_dir.walk(default_allocator);
    defer walker.deinit();

    var buffer: [4096]u8 = undefined;
    var stringBuilder = strings.StringBuilder.init(buffer[0..]);

    var dir_separator: u8 = '/';
    if (builtin.os.tag == .windows) {
        dir_separator = '\\';
    }

    while (try walker.next()) |entry| {
        stringBuilder.reset();
        stringBuilder.append(dir);

        if (stringBuilder.toSlice()[stringBuilder.toSlice().len - 1] != dir_separator) {
            stringBuilder.append(&[1]u8{dir_separator});
        }

        stringBuilder.append(entry.path);

        const target = stringBuilder.toSlice();

        if (!std.mem.endsWith(u8, target, ".pars")) {
            const stat = std.fs.cwd().statFile(target) catch {
                continue;
            };
            if (stat.kind != .file) {
                continue;
            }
            do_create_parity_file(target, null, creation_mode) catch |err| {
                //TODO: Handle errors
                std.debug.print("{any}\n", .{err});
            };
        }
    }
}

fn check_par_file(check_file: []const u8, do_fix: bool) !void {
    const validation_result = ecc.validate_pars_file(check_file, do_fix) catch |err| {
        switch (err) {
            error.IsDir => {
                print("File must not be a directory\n", .{});
            },
            error.AccessDenied => {
                print("Access denied\n", .{});
            },
            else => {
                print("Other error: {any}", .{err});
            },
        }
        return;
    };
    if (validation_result.ok) {
        print("{s}: ok\n", .{check_file});
    } else {
        if (!validation_result.size_ok) {
            print("{s}: file corrupt, size changed, recovery impossible\n", .{check_file});
        } else if (validation_result.unrecoverable_blocks > 0) {
            if (validation_result.recoverable_blocks > 0) {
                print("{s}: file corrupt, found {d} unrecoverable blocks and {d} recoverable blocks\n", .{ check_file, validation_result.unrecoverable_blocks, validation_result.recovered_blocks });
            } else if (validation_result.recovered_blocks > 0) {
                print("{s}: file corrupt, found {d} unrecoverable blocks and recovered {d} blocks\n", .{ check_file, validation_result.unrecoverable_blocks, validation_result.recovered_blocks });
            } else {
                print("{s}: file corrupt, found {d} unrecoverable blocks\n", .{ check_file, validation_result.unrecoverable_blocks });
            }
        } else if (validation_result.recoverable_blocks > 0) {
            print("{s}: file corrupt, recovery possible, found {d} corrupt but recoverable blocks\n", .{ check_file, validation_result.recoverable_blocks });
        } else {
            // If we land here, we must have recovered blocks
            print("{s}: {d} corrupt blocks found, all recovered", .{ check_file, validation_result.recovered_blocks });
        }
    }
}

fn get_par_file_info(pars_info: []const u8) !void {
    const file_header = try ecc.get_pars_file_header(pars_info, default_allocator);
    var blake3_string: [64]u8 = undefined;
    blake_3_as_string(file_header.blake3_hash, &blake3_string);
    print("File name: {s}\nFile Size: {d}\nBlake 3 hash: {s}\nBlock dimension: {d}\nNumber of full size blocks: {d}\nLast block dimension: {d}\n", .{ file_header.file_name, file_header.file_size, blake3_string, file_header.block_dim, file_header.full_size_block_count, file_header.last_block_dim });
}

fn do_create_parity_file(data_file: []const u8, relative_parity_file_location: ?[]const u8, creation_mode: CreationMode) !void {
    switch (creation_mode.creationType) {
        .count => try ecc.create_ecc_file_with_block_count(creation_mode.int_val, data_file, relative_parity_file_location),
        .datausage => try ecc.create_ecc_file_with_datausage(@floatCast(creation_mode.float_val), data_file, relative_parity_file_location),
        .coverage => try ecc.create_ecc_file_with_coverage(creation_mode.int_val, data_file, relative_parity_file_location),
        .dimension => try ecc.create_ecc_file_with_dim(@intCast(creation_mode.int_val), data_file, relative_parity_file_location),
    }
}

fn run_in_watch_mode(file: []const u8, recursive: bool) !void {
    var buffer: [4096]u8 = undefined;

    var string_buffer: [4096]u8 = undefined;
    var stringBuilder = strings.StringBuilder.init(string_buffer[0..]);

    var file_descriptor = try std.os.inotify_init1(0);
    const main_ref = try std.os.inotify_add_watch(file_descriptor, file, IN_CLOSE_WRITE);

    var referenceList = std.ArrayList(DirReference).init(default_allocator);
    defer referenceList.deinit();

    try referenceList.append(DirReference{.id = main_ref, .name = try default_allocator.dupe(u8, file)});

    if (recursive) {
        var it_dir = std.fs.cwd().openIterableDir(file, .{}) catch |err| {
            print("{any}", .{err});
            return;
        };
        defer it_dir.close();

        var walker = try it_dir.walk(default_allocator);
        defer walker.deinit();

        while (try walker.next()) |entry| {
            concat_dir_and_subpath(&stringBuilder, file, entry.path);
            const target = stringBuilder.toSlice();

            const stat = std.fs.cwd().statFile(target) catch {
                continue;
            };
            if (stat.kind == .directory) {
                const ref = try std.os.inotify_add_watch(file_descriptor, target, IN_CLOSE_WRITE);
                const name_dupe = try default_allocator.dupe(u8, target);
                try referenceList.append(DirReference{.id = ref, .name = name_dupe});
            }
        }
    }

    const references = try referenceList.toOwnedSlice();

    print("Started watching '{s}'. Press Control-C to exit application.\n", .{file});
    while (true) {
        const read_size = try std.os.read(file_descriptor, buffer[0..]);
        _ = read_size;
        const short_inotify_len = @sizeOf(std.os.linux.inotify_event);
        var subslice = buffer[0..short_inotify_len];
        var short_inotify: *align(1) std.os.linux.inotify_event = @ptrCast(subslice);
        const file_name_padded: [:0]u8 = @ptrCast(buffer[short_inotify_len .. short_inotify_len + short_inotify.len]);
        const end = std.mem.indexOfSentinel(u8, 0, file_name_padded);
        const file_name = file_name_padded[0..end];

        if (std.mem.endsWith(u8, file_name, ".pars")) {
            continue;
        }

        for (references) |ref| {
            if (ref.id == short_inotify.wd) {
                concat_dir_and_subpath(&stringBuilder, ref.name, file_name);
                const file_location = stringBuilder.toSlice();
                do_create_parity_file(file_location, null) catch |err| {
                    print("{any}\n", .{err});
                };
                print("Created pars file for '{s}'\n", .{file_location});
                break;
            }
        }
    }
}

fn concat_dir_and_subpath(stringBuilder: *strings.StringBuilder, dir: []const u8, subPath: []const u8) void {
    var dir_separator: u8 = '/';
    if (builtin.os.tag == .windows) {
        dir_separator = '\\';
    }
    stringBuilder.reset();
    stringBuilder.append(dir);

    if (stringBuilder.toSlice()[stringBuilder.toSlice().len - 1] != dir_separator) {
        stringBuilder.append(&[1]u8{dir_separator});
    }

    stringBuilder.append(subPath);
}

fn get_int_from_string(str: ?[]const u8) !?u64{
    if (str == null) {
        return null;
    }
    return try std.fmt.parseInt(u64, str.?, 10);
}

fn get_float_from_string(str: ?[]const u8) !?f64 {
    if (str == null) {
        return null;
    }
    return try std.fmt.parseFloat(f64, str.?);
}

fn blake_3_as_string(bytes: [32]u8, output: *[64]u8) void {
    const array = "0123456789abcdef";
    var i: usize = 0;
    while (i < output.len) : (i += 2) {
        var j = i / 2;
        output[i] = array[@as(u4, @truncate(bytes[j] >> 4))];
        output[i + 1] = array[@as(u4, @truncate(bytes[j]))];
    }
}
