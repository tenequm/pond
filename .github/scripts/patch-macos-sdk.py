# Rewrite a macOS binary's LC_BUILD_VERSION.sdk (and legacy LC_VERSION_MIN_MACOSX)
# to 15.0 so macOS 26 dyld tolerates zig's duplicate libobjc load command
# (ziglang/zig#24349 class; the LC_LOAD_DYLIB variant is unfixed upstream).
# Pure Python (Linux has no vtool); handles thin + fat Mach-O, edits in place.
# Assumes zig 0.16 (pinned in the pond-runner image) - re-check on zig bumps.
import struct
import sys

TARGET = 15 << 16  # 15.0.0, encoded X<<16 | Y<<8 | Z
MH_MAGIC_64 = 0xFEEDFACF
FAT_MAGIC, FAT_CIGAM = 0xCAFEBABE, 0xBEBAFECA
LC_BUILD_VERSION, LC_VERSION_MIN_MACOSX = 0x32, 0x24


def patch_thin(buf, base):
    end = "<" if struct.unpack_from("<I", buf, base)[0] == MH_MAGIC_64 else ">"
    ncmds = struct.unpack_from(end + "I", buf, base + 16)[0]
    off = base + 32  # sizeof(mach_header_64)
    for _ in range(ncmds):
        cmd, cmdsize = struct.unpack_from(end + "II", buf, off)
        if cmd == LC_BUILD_VERSION:
            struct.pack_into(end + "I", buf, off + 16, TARGET)  # after cmd,size,platform,minos
        elif cmd == LC_VERSION_MIN_MACOSX:
            struct.pack_into(end + "I", buf, off + 12, TARGET)  # after cmd,size,version
        off += cmdsize


data = bytearray(open(sys.argv[1], "rb").read())
if struct.unpack_from(">I", data, 0)[0] in (FAT_MAGIC, FAT_CIGAM):
    for i in range(struct.unpack_from(">I", data, 4)[0]):
        patch_thin(data, struct.unpack_from(">I", data, 8 + i * 20 + 8)[0])
else:
    patch_thin(data, 0)
open(sys.argv[1], "wb").write(data)
