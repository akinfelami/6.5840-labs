#!/usr/bin/env bash
# Remove only MapReduce intermediate files: mr-<mapTaskID>-<reduceID>
set -euo pipefail

dir="${1:-.}"
shopt -s nullglob

removed=0
for f in "$dir"/mr-[0-9]*-[0-9]*; do
  base=$(basename "$f")
  if [[ "$base" =~ ^mr-[0-9]+-[0-9]+$ ]] && [[ -f "$f" ]]; then
    rm -- "$f"
    removed=$((removed + 1))
  fi
done

echo "removed $removed intermediate file(s) from $dir"
