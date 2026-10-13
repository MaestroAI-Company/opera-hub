#!/bin/sh
# installs or updates opera hub for this user on linux and macos
# curl -fsSL https://github.com/MaestroAI-Company/opera-hub/releases/latest/download/install.sh | sh
set -eu

BASE="https://github.com/MaestroAI-Company/opera-hub/releases/latest/download"
DIR="${OPERA_HUB_DIR:-$HOME/.local/bin}"

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) ASSET=opera-hub-linux-x86_64 ;;
  Linux-aarch64 | Linux-arm64) ASSET=opera-hub-linux-aarch64 ;;
  Darwin-arm64) ASSET=opera-hub-macos-aarch64 TRAY=opera-hubw-macos-aarch64 ;;
  *)
    echo "no Opera Hub build for $(uname -s) $(uname -m)" >&2
    exit 1
    ;;
esac

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fetch() {
  curl -fsSL "$BASE/$1" -o "$TMP/$1"
}

# every file must match the release checksums
verify() {
  expected="$(grep " \*\{0,1\}$1\$" "$TMP/SHA256SUMS" | cut -d ' ' -f 1)"
  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$TMP/$1" | cut -d ' ' -f 1)"
  else
    actual="$(shasum -a 256 "$TMP/$1" | cut -d ' ' -f 1)"
  fi
  if [ -z "$expected" ] || [ "$expected" != "$actual" ]; then
    echo "checksum mismatch for $1, nothing installed" >&2
    exit 1
  fi
}

echo "downloading Opera Hub"
fetch SHA256SUMS
fetch "$ASSET"
verify "$ASSET"
if [ -n "${TRAY:-}" ]; then
  fetch "$TRAY"
  verify "$TRAY"
fi

mkdir -p "$DIR"
# a running hub keeps its old file open
"$DIR/opera-hub" service uninstall >/dev/null 2>&1 || true
install -m 755 "$TMP/$ASSET" "$DIR/opera-hub"
if [ -n "${TRAY:-}" ]; then
  install -m 755 "$TMP/$TRAY" "$DIR/opera-hubw"
fi
"$DIR/opera-hub" service install

echo "Opera Hub is installed in $DIR and starts with your session"
echo "pair a device with: $DIR/opera-hub pair"
case ":$PATH:" in
  *":$DIR:"*) ;;
  *) echo "add $DIR to your PATH to type opera-hub directly" ;;
esac
