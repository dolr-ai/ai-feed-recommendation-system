#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <user> <public-key-file>" >&2
  exit 1
}

if [[ $# -ne 2 ]]; then
  usage
fi

target_user="$1"
public_key_file="$2"

if [[ ! -f "$public_key_file" ]]; then
  echo "Public key file not found: $public_key_file" >&2
  exit 1
fi

target_home="$(getent passwd "$target_user" | cut -d: -f6)"
if [[ -z "$target_home" ]]; then
  echo "User not found: $target_user" >&2
  exit 1
fi
target_group="$(id -gn "$target_user")"

ssh_dir="$target_home/.ssh"
authorized_keys="$ssh_dir/authorized_keys"
tmp_combined="$(mktemp)"
tmp_deduped="$(mktemp)"

cleanup() {
  rm -f "$tmp_combined" "$tmp_deduped"
}
trap cleanup EXIT

sudo install -d -m 700 -o "$target_user" -g "$target_group" "$ssh_dir"

if [[ -f "$authorized_keys" ]]; then
  sudo cat "$authorized_keys" > "$tmp_combined"
  printf '\n' >> "$tmp_combined"
fi
cat "$public_key_file" >> "$tmp_combined"
printf '\n' >> "$tmp_combined"

awk 'NF && !seen[$0]++' "$tmp_combined" > "$tmp_deduped"
sudo install -m 600 -o "$target_user" -g "$target_group" "$tmp_deduped" "$authorized_keys"

echo "Updated $authorized_keys without removing existing keys."
echo "Current key count: $(wc -l < "$tmp_deduped")"
