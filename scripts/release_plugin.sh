#!/bin/bash
set -e

# Usage: ./scripts/release_plugin.sh <version>
# Example: ./scripts/release_plugin.sh 0.1.2

VERSION=$1
PLUGIN_DIR="grafana/plugins/dev-health-panels"

if [ -z "$VERSION" ]; then
  echo "Usage: $0 <version>"
  exit 1
fi

# Ensure we are at the root
if [ ! -d "$PLUGIN_DIR" ]; then
  echo "Error: Plugin directory $PLUGIN_DIR not found. Run from project root."
  exit 1
fi

# Bump version in package.json
echo "Bumping version to $VERSION in $PLUGIN_DIR..."
cd "$PLUGIN_DIR"
# Updates package.json and package-lock.json
npm version "$VERSION" --no-git-tag-version
cd - > /dev/null

# Commit
git add "$PLUGIN_DIR/package.json" "$PLUGIN_DIR/package-lock.json"
git commit -m "chore(release): bump plugin version to $VERSION"

# Tag
TAG="v$VERSION"
echo "Creating tag $TAG..."
git tag "$TAG"

echo "✅ Release $VERSION prepared."
echo "👉 Run the following to push:"
echo "   git push && git push origin $TAG"
