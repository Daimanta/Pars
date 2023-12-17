const std = @import("std");
const fs = std.fs;

const File = fs.File;

const default_allocator = std.heap.page_allocator;

const FileHeader = struct {
    file_size: u64,
    file_name_length: u16,
    file_name: []const u8,
    blake3_hash: [16]u8,
    block_dim: u32
};

const FileBlock = struct {
    index: u64,
    crc: u32,
    col: []u8,
    row: []u8,
};

pub fn create_ecc_file_with_block_count(count: u64, file_path: []const u8, target_file_path: ?[]const u8) !void {
    if (count == 0) {
        return error.CountMustBePositive;
    }
    const file = try fs.cwd().openFile(file_path, .{.mode = .read_only});
    defer file.close();
    const size = try file.getEndPos();
    const block_size = size / count;
    const block_size_float: f64 = @floatFromInt(block_size);
    const target_dim: u32 = @intFromFloat(@ceil(@sqrt(block_size_float)));
    create_ecc_file_with_dim_intern(target_dim, file, target_file_path);
}

pub fn create_ecc_file_with_datausage(usage: f32, file_path: []const u8, target_file_path: ?[]const u8) !void {
    if (usage <= 0.0) {
        return error.UsageMustBePositive;
    }
    if (usage > 1.0) {
        return error.UsageMustNotBeLargerThanOne;
    }

    const target_dim: u32 = @intFromFloat(@ceil(@sqrt(1/usage)));
    try create_ecc_file_with_dim(target_dim, file_path, target_file_path);
}

pub fn create_ecc_file_with_coverage(coverage: f32, file_path: []const u8, target_file_path: ?[]const u8) !void {
    if (coverage <= 0.0) {
        return error.ConverageMustBePositive;
    }
    if (coverage > 1.0) {
        return error.CoverageMustNotBeLargerThanOne;
    }

    const target_dim: u32 = @intFromFloat(@ceil(2.0/coverage));
    try create_ecc_file_with_dim(target_dim, file_path, target_file_path);
}

pub fn create_ecc_file_with_dim(dim: u32, file_path: []const u8, target_file_path: ?[]const u8) !void {
    const file = try fs.cwd().openFile(file_path, .{.mode = .read_only});
    defer file.close();
    try create_ecc_file_with_dim_intern(dim, file, target_file_path);
}

fn create_ecc_file_with_dim_intern(dim: u32, file: File, target_file_path: ?[]const u8) !void {
    var used_dim = dim;
    if (used_dim < 2) {
        used_dim = 2;
    }
    const block_size = used_dim * used_dim;
    const size = try file.getEndPos();

    var file_header = FileHeader {
        .file_size = size,
        .file_name_length = 1,
        .file_name = "a",
        .blake3_hash = [1]u8{0} ** 16,
        .block_dim = used_dim,
     };

    std.debug.print("{any}\n", .{file_header});
    var buffer = try default_allocator.alloc(u8, block_size);
    var col_data = try default_allocator.alloc(u8, used_dim);
    var row_data = try default_allocator.alloc(u8, used_dim);

    defer default_allocator.free(buffer);
    defer default_allocator.free(col_data);
    defer default_allocator.free(row_data);

    while (true) {
        const read_size = try file.read(buffer);
        if (read_size == 0) break;
        var i: usize = 0;
        while (i < used_dim): (i += 1) {
            var start = i * used_dim;
            var j: usize = 0;
            var xor: u8 = 0;
            while (j < used_dim): (j += 1) {
                var index = start + j;
                xor ^= buffer[index];
            }
            row_data[i] = xor;
        }
    }

    std.debug.print("{any}\n", .{row_data});

    _ = target_file_path;
}

