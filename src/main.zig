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
    clap.parseParam("-m, --create <FILE>    Creates a parity file for specified data file.") catch unreachable,
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
    const create_parity_file = args.option("-m");
    const watch_mode = args.option("-w");

    var modes_selected: u8 = 0;
    if (check_file != null) modes_selected += 1;
    if (pars_info != null) modes_selected += 1;
    if (create_parity_file != null) modes_selected += 1;
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
    } else if (create_parity_file != null) {
        mode = .create;
    } else if (watch_mode != null) {
        mode = .watch;
    } else {
        unreachable;
    }

    if (pars_info != null) {
        const file_header = try ecc.get_pars_file_header(pars_info.?);
        std.debug.print("{any}\n", .{file_header});
    }


    //try ecc.create_ecc_file_with_datausage(0.01, "testfiles/tagelied.txt", null);
    //try ecc.validate_pars_file("./testfiles/tagelied.txt.pars", true);
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