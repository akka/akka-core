#!/bin/bash

VERSION=$1
if [ -z $VERSION ]
then
  echo specify the version name to be released, eg. 2.7.0
else
  # 2.10.20 -> 2.10
  MINOR_VERSION=${VERSION%.*}
  sed -e 's/\$VERSION\$/'$VERSION'/g' -e 's/\$MINOR_VERSION\$/'$MINOR_VERSION'/g' akka-docs/release-train-issue-template.md > /tmp/release-$VERSION.md
  echo Created $(gh issue create --title "Release $VERSION" --body-file /tmp/release-$VERSION.md --milestone $VERSION --web)
fi
