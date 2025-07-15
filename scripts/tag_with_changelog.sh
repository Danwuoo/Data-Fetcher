#!/usr/bin/env bash
set -e

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <tag> <message>" >&2
  exit 1
fi

TAG=$1
shift
MESSAGE="$*"

# 建立標籤
git tag -a "$TAG" -m "$MESSAGE"

# 追加紀錄到 CHANGELOG
DATE=$(date -u +%Y-%m-%d)
echo "- ${TAG}: ${MESSAGE} (${DATE})" >> "$(dirname "$0")/../docs/CHANGELOG.md"
