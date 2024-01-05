const std = @import("std");

const clap = @import("clap.zig");
const ecc = @import("ecc.zig");
const version = @import("version.zig");

const default_allocator = std.heap.page_allocator;

const Mode = enum {
    check,
    info,
    create,
    watch
};

const params = [_]clap.Param(clap.Help){
    clap.parseParam("-c, --check <FILE>     Checks a parity file for data file consistency. Cannot be combined with -imw.") catch unreachable,
    clap.parseParam("--dir <DIR>    Standard dir location for creating parity files in watch mode. Default is '.'.") catch unreachable,
    clap.parseParam("-f, --fix     Try to fix data errors found in data files based on parity files. Only works when checking a file.") catch unreachable,
    clap.parseParam("-i, --info <FILE>     Gets header statistics from a parity file. Cannot be combined with -cmw") catch unreachable,
    clap.parseParam("-m, --create <FILE>... Creates a parity file for specified data file. First argument is data file. Second optional argument is parity file location. Default is [datafilelocation].pars") catch unreachable,
    clap.parseParam("-t, --target <FILE>    Specifies the target location file name for the parity file. Default is [data_file_name].pars") catch unreachable,
    clap.parseParam("-w, --watch <FILE>    Continuously watch a directory or file and constructs pars files on updates. Cannot be combined with -cim") catch unreachable,
    clap.parseParam("-h, --help             Display this help and exit.") catch unreachable,
    clap.parseParam("-v, --version     Display the version number and exit.") catch unreachable,
};

pub fn main() !void {

    var iter = try clap.args.OsIterator.init(default_allocator);
    defer iter.deinit();

    var diag = clap.Diagnostic{};
    var args = clap.parse(clap.Help, &params, .{ .diagnostic = &diag }) catch |err| {
        // Report 'Invalid argument [arg]'
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

    var modes_selected: u8 = 0;
    if (check_file != null) modes_selected += 1;
    if (pars_info != null) modes_selected += 1;
    if (create_parity_file.len > 0) modes_selected += 1;
    if (watch_mode != null) modes_selected += 1;
    
    if (modes_selected != 1) {
        try print_help();
        return;
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

    try execute_functionality(mode, check_file, pars_info, create_parity_file, watch_mode);
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

fn execute_functionality(mode: Mode, check_file: ?[]const u8, pars_info: ?[]const u8, create_parity_file: []const []const u8, watch_mode: ?[]const u8) !void{
    switch (mode) {
        .check => {
            try check_par_file(check_file.?);
        },
        .info => {
            try get_par_file_info(pars_info.?);
        },
        .create => {
            try do_create_parity_file(create_parity_file);
        },
        .watch => {
            try run_in_watch_mode(watch_mode.?);
        }
    }

}

fn check_par_file(check_file: []const u8) !void{
    _ = check_file;
}

fn get_par_file_info(pars_info: []const u8) !void {
    const file_header = try ecc.get_pars_file_header(pars_info, default_allocator);
    var blake3_string: [64]u8 = undefined;
    blake_3_as_string(file_header.blake3_hash, &blake3_string);
    print("File name: {s}\nFile Size: {d}\nBlake 3 hash: {s}\nBlock dimension: {d}\nNumber of full size blocks: {d}\nLast block dimension: {d}\n", .{file_header.file_name, file_header.file_size, blake3_string, file_header.block_dim, file_header.full_size_block_count, file_header.last_block_dim});
}

fn do_create_parity_file(parity_file: []const []const u8) !void {
    if (parity_file.len == 0 or parity_file.len > 2) {
        return error.AtLeastOneFileRequired;
    }
    if (parity_file.len == 1) {
        try ecc.create_ecc_file_with_datausage(0.01, parity_file[0], null);
    } else {
        try ecc.create_ecc_file_with_datausage(0.01, parity_file[0], parity_file[1]);
    }
}

fn run_in_watch_mode(watch_mode: []const u8) ! void {
    _ = watch_mode;
}

fn blake_3_as_string(bytes: [32]u8, output: *[64]u8) void{
    const array = "0123456789abcdef";
    var i: usize = 0;
    while (i < output.len) : (i += 2) {
        var j = i / 2;
        output[i] = array[@as(u4, @truncate(bytes[j] >> 4))];
        output[i + 1] = array[@as(u4, @truncate(bytes[j]))];
    }
}
