#!/bin/sh
# gp3-shape.sh — entrypoint wrapper that throttles this container's block I/O to
# AWS EBS gp3 characteristics, then exec's the real command. Makes a fast laptop
# NVMe behave like gp3 (125 MB/s, 3000 IOPS) so the VL/VT disk baseline in the
# benchmark is production-faithful instead of flatteringly fast.
#
# No-op unless DISK_PROFILE=gp3-loop. Requires: privileged container + cgroup v2
# io controller + util-linux (losetup/lsblk) + e2fsprogs (mkfs.ext4). Validated
# on Docker Desktop: 8.1 GB/s -> 131 MB/s.
set -e
: "${DISK_PROFILE:=local-ssd}"
: "${GP3_BW_BYTES:=131072000}"      # 125 MB/s
: "${GP3_IOPS:=3000}"
: "${SHAPE_DATA_PATH:=/data}"
: "${SHAPE_BACKING:=/shape/gp3.img}"
: "${SHAPE_SIZE_MB:=4096}"

if [ "$DISK_PROFILE" != "gp3-loop" ]; then
  exec "$@"
fi

mkdir -p "$(dirname "$SHAPE_BACKING")" "$SHAPE_DATA_PATH"
[ -f "$SHAPE_BACKING" ] || dd if=/dev/zero of="$SHAPE_BACKING" bs=1M count="$SHAPE_SIZE_MB" status=none
LOOP=$(losetup -f --show "$SHAPE_BACKING")
MAJMIN=$(lsblk -no MAJ:MIN "$LOOP" | head -1 | tr -d ' ')
mkfs.ext4 -q -F "$LOOP"
mount "$LOOP" "$SHAPE_DATA_PATH"
echo "+io" > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || true
echo "$MAJMIN wbps=$GP3_BW_BYTES rbps=$GP3_BW_BYTES wiops=$GP3_IOPS riops=$GP3_IOPS" > /sys/fs/cgroup/io.max
echo "[gp3-shape] $SHAPE_DATA_PATH on $LOOP ($MAJMIN) capped: [$(grep "$MAJMIN" /sys/fs/cgroup/io.max)]" >&2
exec "$@"
