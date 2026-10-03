#!/usr/bin/env bash
# Build AmneziaWG for Pi5 on the laptop (ADR-0012): the userspace daemon amneziawg-go and the
# tools awg and awg-quick. Pi5 has no GitHub access (ADR-0007) and no kernel module, so the
# binaries are built here and shipped by install-awg.sh with their sha256.
#   scripts/pi5/build-awg.sh            # linux/arm64 -> build/awg/arm64/
#   ARCH=amd64 scripts/pi5/build-awg.sh # the same for the laptop, used by the local test only
# Sources are pinned by tag AND full commit: a moved tag fails the build.
set -euo pipefail
cd "$(dirname "$0")/../.."

GO_REF=v3.1.20260828 GO_SHA=b5928efb6ca19f0153958460c3d141f04abc5c2e
TOOLS_REF=v3.1.20260812 TOOLS_SHA=ee0f0a9aa34ff0a0da4b3433b9512781cfe02843
IMAGE=golang:1.25-bookworm
ARCH=${ARCH:-arm64}
case $ARCH in
    arm64) CROSS=aarch64-linux-gnu- PKGS="gcc-aarch64-linux-gnu libc6-dev-arm64-cross make" ;;
    amd64) CROSS= PKGS="gcc make" ;;
    *) echo "ARCH must be arm64 or amd64" >&2; exit 2 ;;
esac
OUT=build/awg/$ARCH
mkdir -p "$OUT"

docker run --rm \
    -e ARCH="$ARCH" -e CROSS="$CROSS" -e PKGS="$PKGS" -e GO_REF="$GO_REF" -e GO_SHA="$GO_SHA" \
    -e TOOLS_REF="$TOOLS_REF" -e TOOLS_SHA="$TOOLS_SHA" -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
    -v "$PWD/$OUT:/out" "$IMAGE" bash -euo pipefail -c '
fetch() {  # name url ref sha
    git clone -q --depth 1 --branch "$3" "$2" "/src/$1"
    [[ $(git -C "/src/$1" rev-parse HEAD) == "$4" ]] || { echo "$1 $3 is not commit $4" >&2; exit 1; }
}
fetch go https://github.com/amnezia-vpn/amneziawg-go "$GO_REF" "$GO_SHA"
fetch tools https://github.com/amnezia-vpn/amneziawg-tools "$TOOLS_REF" "$TOOLS_SHA"

(cd /src/go && CGO_ENABLED=0 GOOS=linux GOARCH=$ARCH go build -trimpath -ldflags "-s -w" -o /out/amneziawg-go .)

apt-get update -qq >/dev/null && apt-get install -y -qq --no-install-recommends $PKGS >/dev/null
make -C /src/tools/src -s wg CC="${CROSS}gcc" LDFLAGS="-static" >/dev/null
cp /src/tools/src/wg /out/awg
cp /src/tools/src/wg-quick/linux.bash /out/awg-quick
chmod 755 /out/amneziawg-go /out/awg /out/awg-quick
printf "amneziawg-go %s %s\namneziawg-tools %s %s\narch %s\n" "$GO_REF" "$GO_SHA" "$TOOLS_REF" "$TOOLS_SHA" "$ARCH" > /out/VERSIONS
(cd /out && sha256sum amneziawg-go awg awg-quick > SHA256SUMS)
chown -R "$HOST_UID:$HOST_GID" /out
'
echo "built $OUT:"
cat "$OUT/VERSIONS"
(cd "$OUT" && sha256sum -c SHA256SUMS && file amneziawg-go awg 2>/dev/null | cut -c1-150 || true)
