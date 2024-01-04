const std = @import("std");
const fs = std.fs;
const blake3 = std.crypto.hash.Blake3;
const crc32 = std.hash.Crc32;
const testing = std.testing;

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

    fn get_total_blocks(self: *FileHeader) u64 {
        var result = self.full_size_block_count;
        if (result.last_block_dim > 0) result += 1;
        return result;
    }

    fn file_size_matches(self: *FileHeader, file_size: usize) bool {
        // Start and end file type marker + header size(without file name length)
        var expected: usize = 4 + 8 + 16 + 4 + 8 + 4 + 2 + 4;
        expected += self.file_name_length;
        expected += ((self.block_dim * self.block_dim) + 4) * self.full_size_block_count;
        if (self.last_block_dim > 0) {
            expected += ((self.last_block_dim * self.last_block_dim) + 4);
        }
        return expected == file_size;
    }
};

const FileBlock = struct {
    crc: u32,
    col: []u8,
    row: []u8,
    dim: usize,
};

const VerificationResult = struct {
    file_has_full_integrity: bool,
    size_matches: bool,
    total_blocks: u64,
    faulty_blocks: u64,
    fixable_faulty_blocks: u64
};

pub fn create_ecc_file_with_block_count(count: u64, file_path: []const u8, target_file_path: ?[]const u8) !void {
    if (count == 0) {
        return error.CountMustBePositive;
    }
    const file = try fs.cwd().openFile(file_path, .{.mode = .read_only});
    defer file.close();
    const size = try file.getEndPos();
    const block_size = size / count;
    const target_dim = get_smallest_dim_to_contain_size(block_size);
    create_ecc_file_with_dim_intern(target_dim, file, file_path, target_file_path);
}

pub fn create_ecc_file_with_datausage(usage: f32, file_path: []const u8, target_file_path: ?[]const u8) !void {
    if (usage <= 0.0) {
        return error.UsageMustBePositive;
    }
    if (usage > 1.0) {
        return error.UsageMustNotBeLargerThanOne;
    }

    const block_size = 1/usage;
    const target_dim: u32 = get_smallest_dim_to_contain_size(block_size);
    try create_ecc_file_with_dim(target_dim, file_path, target_file_path);
}

pub fn create_ecc_file_with_coverage(coverage: f32, file_path: []const u8, target_file_path: ?[]const u8) !void {
    if (coverage <= 0.0) {
        return error.CoverageMustBePositive;
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

pub fn validate_pars_file(target_file_path: []const u8, try_fix_data_file: bool) !void {
    var par_file = try fs.cwd().openFile(target_file_path, .{.mode = .read_only});
    var header = try get_header_block(par_file);
    defer default_allocator.free(header.file_name);

    const data_file_path = try get_data_file_path(target_file_path, header.file_name);
    defer default_allocator.free(data_file_path);
    var data_file = try fs.cwd().openFile(data_file_path, .{.mode = .read_write});

    if (header.file_size_matches(try par_file.getEndPos())) {
        return error.ParFileSizeMismatch;
    }

    // If the size is mismatched, any form of matching is potentially difficult
    // Skip for now
    if (try data_file.getEndPos() != header.file_size) {
        return error.SizeMismatch;
    }

    var data_file_hash: [16]u8 = undefined;
    try calculate_blake3_hash(data_file, &data_file_hash);

    // If hashes match, data is OK
    if (std.mem.eql(u8, &data_file_hash, &header.blake3_hash)) {
        return;
    }

    // We have work to do
    try par_file.seekTo(46 + header.file_name_length);
    try data_file.seekTo(0);

    var data_block = try default_allocator.alloc(u8, header.block_dim*header.block_dim);
    var parity_block = try default_allocator.alloc(u8, 2*header.block_dim + 4);
    var parity_col_match = try default_allocator.alloc(u8, header.block_dim);
    var parity_row_match = try default_allocator.alloc(u8, header.block_dim);

    defer default_allocator.free(data_block);
    defer default_allocator.free(parity_block);
    defer default_allocator.free(parity_col_match);
    defer default_allocator.free(parity_row_match);

    var i: u64 = 0;
    while (i < header.full_size_block_count): (i += 1) {
        try check_block(data_file, par_file, data_block, parity_block, parity_row_match, parity_col_match, i, header.block_dim, header.block_dim, try_fix_data_file);
    }

    if (header.last_block_dim > 0) {
        try check_block(data_file, par_file, data_block, parity_block, parity_row_match, parity_col_match, header.full_size_block_count, header.block_dim, header.last_block_dim, try_fix_data_file);
    }
}

pub fn get_pars_file_header(target_file_path: []const u8) !FileHeader {
    var par_file = try fs.cwd().openFile(target_file_path, .{.mode = .read_only});
    return try get_header_block(par_file);
}

fn create_ecc_file_with_dim_intern(dim: u32, file: File, file_path: []const u8, target_file_path: ?[]const u8) !void {
    const size = try file.getEndPos();
    var used_dim = dim;
    if (dim < 2) {
        used_dim = 2;
    } else if (size / (dim * dim) == 0) {
        used_dim = get_smallest_dim_to_contain_size(size);
    }
    const block_size = used_dim * used_dim;

    var full_size_block_count: u64 = size / block_size;

    var last_block_size = size - (full_size_block_count * block_size);
    var last_block_dim: u32 = 0;
    if (last_block_size > 0) {
        last_block_dim = @intFromFloat(@ceil(@sqrt(@as(f64, @floatFromInt(last_block_size)))));
    }

    var target_path: []const u8 = undefined;
    var allocated_path: ?[]u8 = null;

    if (target_file_path != null) {
        target_path = target_file_path.?;
    } else {
        allocated_path = try default_allocator.alloc(u8, file_path.len + ".pars".len);
        std.mem.copy(u8, allocated_path.?[0..], file_path);
        std.mem.copy(u8, allocated_path.?[file_path.len..], ".pars");
        target_path = allocated_path.?;
    }
    defer if (allocated_path != null) default_allocator.free(allocated_path.?);

    // Overwrites existing file
    const target_file = try fs.cwd().createFile(target_path, .{});
    defer target_file.close();

    var relative_file_path: []u8 = try fs.path.relative(default_allocator, target_path, file_path);
    std.mem.replaceScalar(u8, relative_file_path, '\\', '/');
    defer default_allocator.free(relative_file_path);


    // Relative path has a ".." at the start, slice into a "." for same dir
    var file_header = FileHeader {
        .file_size = size,
        .file_name_length = @intCast(relative_file_path.len - 1),
        .file_name = relative_file_path[1..],
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
    _ = try target_file.write("SRAP");
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
       if (last_block_dim + 1 < col_data.len) {
            @memset(col_data[last_block_dim+1..], 0);
            @memset(row_data[last_block_dim+1..], 0);
       }

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

fn check_block(data_file: File, par_file: File, data_block: []u8, parity_block: []u8, parity_row_match: []u8, parity_col_match: []u8, block_index: u64, max_dim: u32, dim: u32, try_fix_data_file: bool) !void {
    _ = try data_file.read(data_block);
    _ = try par_file.read(parity_block);
    const data_crc = crc32.hash(data_block[0..]);
    const par_crc = extract_u32(parity_block[0..4].*);
    // If parity doesn't match, then the data is not expected(perhaps corrupted)
    // If parity matches, it might be dumb luck
    if (data_crc == par_crc) return;
    var col_checks = parity_block[4..4+dim];
    var row_checks = parity_block[4+dim..4+dim+dim];

    var j: usize = 0;
    var k: usize = 0;

    while (j < dim): (j += 1) {
        var xor: u8 = 0;
        k = 0;
        while (k < dim): (k += 1) {
            const index: usize = (j * dim) + k;
            xor ^= data_block[index];
        }
        parity_row_match[j] = xor;
    }

    j = 0;
    k = 0;

    while (k < dim): (k += 1) {
        var xor: u8 = 0;
        j = 0;
        while (j < dim): (j += 1) {
            const index: usize = (j * dim) + k;
            xor ^= data_block[index];
        }
        parity_col_match[k] = xor;
    }

    var row_errors: u32 = 0;
    var col_errors: u32 = 0;

    var idx: usize = 0;
    while (idx < dim): (idx += 1) {
        if (row_checks[idx] != parity_row_match[idx]) {
            row_errors += 1;
        }
        if (col_checks[idx] != parity_col_match[idx]) {
            col_errors += 1;
        }
    }

    // We have one byte error in a block
    // Restoration should be trivial
    if (try_fix_data_file and row_errors == 1 and col_errors == 1) {
        var fix_row: usize = undefined;
        var fix_column: usize = undefined;
        idx = 0;
        while (idx < dim): (idx += 1) {
            if (row_checks[idx] != parity_row_match[idx]) {
                fix_row = idx;
                break;
            }
        }
        idx = 0;
        while (idx < dim): (idx += 1) {
            if (col_checks[idx] != parity_col_match[idx]) {
                fix_column = idx;
                break;
            }
        }

        const absolute_index : usize = (block_index * max_dim * max_dim) + (fix_row * dim) + fix_column;

        var corrected_value: u8 = row_checks[fix_row];
        idx = 0;
        while (idx < dim): (idx += 1) {
            const used_index = dim*fix_row + idx;
            if (idx != fix_column) {
                corrected_value ^= data_block[used_index];
            }
        }

        _ = try data_file.pwrite(&[1]u8{corrected_value}, absolute_index);
    }

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

fn get_header_block(file: File) !FileHeader{
    const fileSize = try file.getEndPos();
    if (fileSize < 20) {
        return error.InvalidParsFile;
    }
    var file_marker_buffer: [4]u8 = undefined;
    _ = try file.pread(file_marker_buffer[0..], 0);
    if (!std.mem.eql(u8, "PARS", file_marker_buffer[0..])) {
        return error.InvalidParsFile;
    }
    _ = try file.pread(file_marker_buffer[0..], fileSize - 4);
    if (!std.mem.eql(u8, "SRAP", file_marker_buffer[0..])) {
        return error.InvalidParsFile;
    }

    try file.seekTo(0);
    // 46 bytes from starts is all header information except file length
    var header_buffer: [46]u8 = undefined;
    var read = try file.read(header_buffer[0..]);
    _ = read;
    var header: FileHeader = .{
        .file_size = extract_u64(header_buffer[4..12].*),
        .blake3_hash = undefined,
        .block_dim = extract_u32(header_buffer[28..32].*),
        .full_size_block_count = extract_u64(header_buffer[32..40].*),
        .last_block_dim = extract_u32(header_buffer[40..44].*),
        .file_name_length = extract_u16(header_buffer[44..46].*),
        .file_name = undefined
    };
    std.mem.copy(u8, header.blake3_hash[0..], header_buffer[12..28]);
    var file_name = try default_allocator.alloc(u8, @intCast(header.file_name_length));
    _ = try file.read(file_name[0..]);
    header.file_name = file_name;
    return header;
}

fn get_smallest_dim_to_contain_size(size: usize) u32 {
    const block_size_float: f64 = @floatFromInt(size);
    return @intFromFloat(@ceil(@sqrt(block_size_float)));
}

fn extract_u16(input: [2]u8) u16{
    var cast: *[1]u16 align(1) = @constCast(@alignCast(@ptrCast(&input)));
    return cast[0];
}

fn extract_u32(input: [4]u8) u32{
    var cast: *[1]u32 align(1) = @constCast(@alignCast(@ptrCast(&input)));
    return cast[0];
}

fn extract_u64(input: [8]u8) u64{
    var cast: *[1]u64 align(1) = @constCast(@alignCast(@ptrCast(&input)));
    return cast[0];
}

fn get_data_file_path(pars_file_path: []const u8, data_file_path: []const u8) ![]u8 {
    if (data_file_path[0] == '/') {
        return try default_allocator.dupe(u8, data_file_path);
    }
    const last_slash_index = std.mem.lastIndexOfScalar(u8, pars_file_path, '/').?;
    var result_size = last_slash_index + data_file_path.len + 1;
    var data_file_start_index: usize = 0;
    var result = try default_allocator.alloc(u8, result_size);
    std.mem.copy(u8, result, pars_file_path[0..last_slash_index+1]);
    std.mem.copy(u8, result[last_slash_index+1..], data_file_path[data_file_start_index..]);
    return result;
}