
const ecc = @import("ecc.zig");

pub fn main() !void {
    //try ecc.create_ecc_file_with_datausage(0.01, "testfiles/tagelied.txt", null);
    try ecc.validate_pars_file("./testfiles/tagelied.txt.pars");
}