#!/bin/sh
here="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
sys="$(uname -s)"
case "$sys" in
MINGW* | MSYS* | CYGWIN*) rel="x86_64-pc-windows-msvc/inbound.exe" ;;
Darwin*)
    case "$(uname -m)" in
    arm64) rel="aarch64-apple-darwin/inbound" ;;
    *) rel="x86_64-apple-darwin/inbound" ;;
    esac ;;
*)
    case "$(uname -m)" in
    aarch64 | arm64) rel="aarch64-unknown-linux-musl/inbound" ;;
    *) rel="x86_64-unknown-linux-musl/inbound" ;;
    esac ;;
esac
if [ -x "$here/$rel" ]; then exec "$here/$rel" "$@"; fi
for cand in "$here/target/release/inbound.exe" "$here/target/release/inbound" "$here/target/debug/inbound.exe" "$here/target/debug/inbound"; do
    if [ -x "$cand" ]; then exec "$cand" "$@"; fi
done
echo "inbound binary not found in $here" >&2
exit 1
