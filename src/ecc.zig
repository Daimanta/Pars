const std = @import("std");
const fs = std.fs;
const blake3 = std.crypto.hash.Blake3;
const crc32 = std.hash.Crc32;

const File = fs.File;

const default_allocator = std.heap.page_allocator;

const FileHeader = struct {
    file_size: u64,
    blake3_hash: [16]u8,
    block_dim: u32,
    full_size_block_count: u64,
    last_block_dim: u32,
    file_name_length: u16,
    file_name: []const u8,
};

const FileBlock = struct {
    crc: u32,
    col: []u8,
    row: []u8,
    dim: usize,
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
    create_ecc_file_with_dim_intern(target_dim, file, file_path, target_file_path);
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
    try create_ecc_file_with_dim_intern(dim, file, file_path, target_file_path);
}

fn create_ecc_file_with_dim_intern(dim: u32, file: File, file_path: []const u8, target_file_path: ?[]const u8) !void {
    var used_dim = dim;
    if (used_dim < 2) {
        used_dim = 2;
    }
    const block_size = used_dim * used_dim;
    const size = try file.getEndPos();
    var full_size_block_count: u64 = size / block_size;

    var last_block_size = size - (full_size_block_count * block_size);
    var last_block_dim: u32 = 0;
    if (last_block_size > 0) {
        last_block_dim = @intFromFloat(@ceil(@sqrt(@as(f64, @floatFromInt(last_block_size)))));
    }

    var target_path: []const u8 = undefined;
    if (target_file_path != null) {
        target_path = target_file_path.?;
    } else {
        target_path = try default_allocator.alloc(u8, file_path.len + ".pars".len);
        target_path = "temp.file";
    }

    // Overwrites existing file
    const target_file = try fs.cwd().createFile(target_path, .{});
    defer target_file.close();

    var file_header = FileHeader {
        .file_size = size,
        .file_name_length = @intCast(file_path.len),
        .file_name = file_path,
        .blake3_hash = [1]u8{0} ** 16,
        .block_dim = used_dim,
        .full_size_block_count = full_size_block_count,
        .last_block_dim = last_block_dim
     };

    try calculate_blake3_hash(file, &file_header.blake3_hash);

    try write_header(target_file, file_header);

    var buffer = try default_allocator.alloc(u8, block_size);
    var col_data = try default_allocator.alloc(u8, used_dim);
    var row_data = try default_allocator.alloc(u8, used_dim);

    defer default_allocator.free(buffer);
    defer default_allocator.free(col_data);
    defer default_allocator.free(row_data);
    
    try file.seekTo(0);
    
    while (true) {
        const file_block_opt = try create_block(file, used_dim, last_block_dim, buffer, col_data, row_data);
        if (file_block_opt == null) break;
        const file_block = file_block_opt.?;
        try write_block(target_file, file_block);
    }

}

fn calculate_blake3_hash(file: File, blake3_hash: *[16]u8) !void {
    var blake3_state = blake3.init(blake3.Options{});
    var blake_buffer = try default_allocator.alloc(u8, 1<<20);
    defer default_allocator.free(blake_buffer);
    while (true) {
        const read_size = try file.read(blake_buffer);
        if (read_size == 0) break;
        blake3_state.update(blake_buffer[0..read_size]);
    }
    blake3_state.final(blake3_hash[0..]);
}

fn create_block(file: File, dim: u32, last_block_dim: u32, buffer: []u8, col_data: []u8, row_data: []u8) !?FileBlock {
   const read_size = try file.read(buffer);
   if (read_size == 0) return null;
   var block_dim = dim;
   if (read_size < buffer.len) {
       block_dim = last_block_dim;
   }

   var crc: u32 = crc32.hash(buffer[0..read_size]);
   var i: usize = 0;
   var j: usize = 0;
   while (i < block_dim): (i += 1) {
       var start = i * block_dim;
       var xor: u8 = 0;
       j = 0;
       while (j < block_dim): (j += 1) {
           var index = start + j;
           xor ^= buffer[index];
       }
       row_data[i] = xor;
   }
   i = 0;
   j = 0;
   while (j < block_dim): (j += 1) {
       var start = j;
       var xor: u8 = 0;
       i = 0;
       while (i < block_dim): (i += 1) {
           var index = start + (i * block_dim);
           xor ^= buffer[index];
       }
       col_data[j] = xor;
   }

   var result = FileBlock{.crc = crc, .row = row_data, .col = col_data, .dim = block_dim};
   return result;
}

fn write_header(file: File, header: FileHeader) !void {
    _ = try file.write("PARS");
    _ = try file.write(std.mem.sliceAsBytes(&[1]u64{header.file_size}));
    _ = try file.write(header.blake3_hash[0..]);
    _ = try file.write(std.mem.sliceAsBytes(&[1]u32{header.block_dim}));
    _ = try file.write(std.mem.sliceAsBytes(&[1]u64{header.full_size_block_count}));
    _ = try file.write(std.mem.sliceAsBytes(&[1]u32{header.last_block_dim}));
    _ = try file.write(std.mem.sliceAsBytes(&[1]u16{header.file_name_length}));
    _ = try file.write(header.file_name);
}

fn write_block(file: File, block: FileBlock) !void {
    _ = try file.write(std.mem.sliceAsBytes(&[1]u32{block.crc}));
    _ = try file.write(block.col[0..block.dim]);
    _ = try file.write(block.row[0..block.dim]);
}
