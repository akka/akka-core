#!/usr/bin/env bash
#
# release-prep.sh [--commit-and-pr] <akka-version>
#
# Prepares the repository for a release by:
#  - Updating the LICENSE file:
#      * "Licensed Work: Akka X.Y.Z"   -> the given akka version
#      * "(c) YYYY Lightbend Inc."     -> the current year
#      * "Change Date: YYYY-MM-DD"     -> today + 3 years
#  - Bumping the akka version in every sample build file:
#      * samples/**/build.sbt    (literal val and sys.props.getOrElse forms)
#      * samples/**/pom.xml      (<akka.version>...)
#      * samples/**/build.gradle ('com.typesafe.akka:<artifact>:<version>')
#
# By default the script only updates the files; review with `git diff` and
# commit yourself. With --commit-and-pr it creates a wip-release-prep-X.Y.Z
# branch off of the currently checked-out branch, applies the changes,
# commits, pushes, and opens a pull request via the gh CLI.

set -euo pipefail

usage() {
  echo "Usage: $0 [--commit-and-pr] <akka-version>" >&2
  echo "Example: $0 2.10.18" >&2
  echo "         $0 --commit-and-pr 2.10.18" >&2
}

COMMIT_AND_PR=false
POSITIONAL=()
while [ $# -gt 0 ]; do
  case "$1" in
    --commit-and-pr) COMMIT_AND_PR=true; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; POSITIONAL+=("$@"); break ;;
    -*) echo "Unknown flag: $1" >&2; usage; exit 1 ;;
    *) POSITIONAL+=("$1"); shift ;;
  esac
done
if [ ${#POSITIONAL[@]} -ne 1 ]; then
  usage
  exit 1
fi

AKKA_VERSION="${POSITIONAL[0]}"
COMMIT_MSG="chore: License change date and sample bump for $AKKA_VERSION"

# Cross-platform date arithmetic: BSD (macOS) and GNU (Linux) take different flags.
if date -v+0d >/dev/null 2>&1; then
  CHANGE_DATE=$(date -v+3y +%Y-%m-%d)
else
  CHANGE_DATE=$(date -d '+3 years' +%Y-%m-%d)
fi
CURRENT_YEAR=$(date +%Y)

# Cross-platform in-place sed: BSD requires an explicit backup-extension argument
# (empty = no backup), GNU does not accept the empty argument. Stored as an
# array so it composes with both direct calls and `find -exec`.
if sed --version >/dev/null 2>&1; then
  SED_INPLACE=(sed -i)
else
  SED_INPLACE=(sed -i '')
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# When opening a PR we need a clean tree, the gh CLI, and a free branch name.
if [ "$COMMIT_AND_PR" = true ]; then
  command -v gh >/dev/null 2>&1 || {
    echo "gh CLI not found; install from https://cli.github.com" >&2
    exit 1
  }
  if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Working tree is not clean; commit or stash changes first." >&2
    exit 1
  fi
  TARGET_BASE=$(git rev-parse --abbrev-ref HEAD)
  if [ "$TARGET_BASE" = "HEAD" ]; then
    echo "Detached HEAD; check out the target branch (e.g. main or release-X.Y) first." >&2
    exit 1
  fi
  NEW_BRANCH="wip-release-prep-$AKKA_VERSION"
  if git rev-parse --verify "$NEW_BRANCH" >/dev/null 2>&1; then
    echo "Branch $NEW_BRANCH already exists; aborting." >&2
    exit 1
  fi
  echo "Creating branch $NEW_BRANCH (PR will target $TARGET_BASE)"
  git checkout -b "$NEW_BRANCH"
  echo
fi

echo "Preparing release of Akka $AKKA_VERSION"
echo "  Copyright year: $CURRENT_YEAR"
echo "  Change Date:    $CHANGE_DATE (today + 3 years)"
echo

echo "Updating LICENSE..."
"${SED_INPLACE[@]}" -E \
  -e "s/^(Licensed Work:[[:space:]]+Akka )[^[:space:]]+/\\1$AKKA_VERSION/" \
  -e "s/(The Licensed Work is \\(c\\) )[0-9]{4}( Lightbend Inc\\.)/\\1$CURRENT_YEAR\\2/" \
  -e "s/^(Change Date:[[:space:]]+)[0-9-]+/\\1$CHANGE_DATE/" \
  LICENSE

echo "Bumping samples to $AKKA_VERSION..."
# build.sbt: handle both the literal `val AkkaVersion = "..."` form and the
# sys.props-aware `lazy val akkaVersion = sys.props.getOrElse("akka.version", "...")` form.
find samples -name build.sbt -exec "${SED_INPLACE[@]}" -E \
  -e "s/^val AkkaVersion = \"[^\"]*\"/val AkkaVersion = \"$AKKA_VERSION\"/" \
  -e "s/^lazy val akkaVersion = sys\\.props\\.getOrElse\\(\"akka\\.version\", \"[^\"]*\"\\)/lazy val akkaVersion = sys.props.getOrElse(\"akka.version\", \"$AKKA_VERSION\")/" \
  {} +

# pom.xml: the <akka.version> property.
find samples -name pom.xml -exec "${SED_INPLACE[@]}" -E \
  -e "s|<akka\\.version>[^<]*</akka\\.version>|<akka.version>$AKKA_VERSION</akka.version>|" \
  {} +

# build.gradle: every 'com.typesafe.akka:<artifact>:<version>' coordinate.
find samples -name build.gradle -exec "${SED_INPLACE[@]}" -E \
  -e "s|('com\\.typesafe\\.akka:[^:']+:)[^']*'|\\1$AKKA_VERSION'|g" \
  {} +

if [ "$COMMIT_AND_PR" = true ]; then
  echo
  echo "Committing..."
  git add LICENSE samples
  git commit -m "$COMMIT_MSG"
  echo "Pushing $NEW_BRANCH..."
  git push -u origin "$NEW_BRANCH"
  echo "Opening pull request..."
  gh pr create --base "$TARGET_BASE" --title "$COMMIT_MSG" --body "$COMMIT_MSG"
  echo "Done."
else
  echo
  echo "Done. Review the changes with 'git diff' and commit when ready."
  echo "(Re-run with --commit-and-pr to commit, push, and open a PR automatically.)"
fi
