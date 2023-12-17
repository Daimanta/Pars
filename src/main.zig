
const ecc = @import("ecc.zig");

pub fn main() !void {
    try ecc.create_ecc_file_with_datausage(0.01, "testfiles/tagelied.txt", null);
}