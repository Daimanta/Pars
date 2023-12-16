const std = @import("std");
const fs = std.fs;

const File = fs.File;

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
    create_ecc_file_with_dim(target_dim, file_path, target_file_path);
}

pub fn create_ecc_file_with_coverage(coverage: f32, file_path: []const u8, target_file_path: ?[]const u8) !void {
    if (coverage <= 0.0) {
        return error.ConverageMustBePositive;
    }
    if (coverage > 1.0) {
        return error.CoverageMustNotBeLargerThanOne;
    }

    const target_dim: u32 = @intFromFloat(@ceil(2.0/coverage));
    create_ecc_file_with_dim(target_dim, file_path, target_file_path);
}

pub fn create_ecc_file_with_dim(dim: u32, file_path: []const u8, target_file_path: ?[]const u8) !void {
    const file = try fs.cwd().openFile(file_path, .{.mode = .read_only});
    defer file.close();
    create_ecc_file_with_dim_intern(dim, file, target_file_path);
}

fn create_ecc_file_with_dim_intern(dim: u32, file: File, target_file_path: ?[]const u8) !void {
    var used_dim = dim;
    if (used_dim < 2) {
        used_dim = 2;
    }
    _ = target_file_path;
    const size = try file.getEndPos();
    _ = size;
}

