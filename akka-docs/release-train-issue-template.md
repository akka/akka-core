Release Akka $VERSION$

<!--
# Release Train Issue Template for Akka

(Liberally copied and adopted from Scala itself https://github.com/scala/scala-dev/blob/b11cd2e4a4431de7867db6b39362bea8fa6650e7/notes/releases/template.md)

For every release, use the `scripts/create-release-issue.sh` to make a copy of this file named after the release, and expand the variables.

Variables to be expanded in this template:
- $VERSION$=???

Key links:
  - akka/akka-core milestone: https://github.com/akka/akka-core/milestone/?
-->

### Cutting the release

- [ ] Check that open PRs and issues assigned to the milestone are reasonable
- [ ] If PRs related to clustering were merged after the previous multi-node test run, trigger the [multi-node tests](https://github.com/akka/akka-core/actions/workflows/multi-node.yml) and see that they are green.
- [ ] If PRs were merged after EU midnight, trigger the [native-image tests](https://github.com/akka/akka-core/actions/workflows/native-image-tests.yml) and see that they are green.
- [ ] Update the version and change date in the LICENSE file.
- [ ] Update the Akka version in the samples to $VERSION$, otherwise the published zip files of the samples will have the old version.
- [ ] Create a new milestone for the [next version](https://github.com/akka/akka-core/milestones)
- [ ] Close the [$VERSION$ milestone](https://github.com/akka/akka-core/milestones?direction=asc&sort=due_date)
- [ ] Make sure all important PRs have been merged
- [ ] Wait until [main build finished](https://github.com/akka/akka-core/actions) after merging the latest PR
- [ ] Update the [draft release](https://github.com/akka/akka-core/releases) with the next tag version `v$VERSION$`, title and release description. Use the `Publish release` button, which will create the tag.
- [ ] Check that GitHub Actions release build has executed successfully (GitHub Actions will start a [CI build](https://github.com/akka/akka-core/actions) for the new tag and publish artifacts to https://repo.akka.io/TOKEN/secure)

### Check availability

- [ ] Check [API](https://doc.akka.io/api/akka-core/$VERSION$/) documentation
- [ ] Check [reference](https://doc.akka.io/libraries/akka-core/$VERSION$/) documentation. Check that the reference docs were deployed and show a version warning (see section below on how to fix the version warning).
- [ ] Check the release on https://repo.akka.io/TOKEN/secure/com/typesafe/akka/akka-actor_2.13/$VERSION$/akka-actor_2.13-$VERSION$.pom

### When everything is on https://repo.akka.io/TOKEN/secure
  - [ ] Log into `gustav.akka.io` as `akkarepo` 
    - [ ] If this updates the `current` version, run `./update-akka-current-version.sh $VERSION$`
    - [ ] otherwise check changes and commit the new version to the local git repository
         ```
         cd ~/www
         git status
         git add libraries/akka-core/current libraries/akka-core/$VERSION$
         git add api/akka-core/current api/akka-core/$VERSION$
         git add japi/akka-core/current japi/akka-core/$VERSION$
         git commit -m "Akka core $VERSION$"
         ```
    - [ ] push changes to the [remote git repository](https://github.com/akka/doc.akka.io)
         ```
         cd ~/www
         git push origin master
         ```
  - [ ] If this updated 'current' docs - trigger a re-index of the docs for search through [Run workflow for the scraper](https://github.com/akka/akka-core/actions/workflows/algolia-doc-site-scrape.yml)
  

### Announcements

- Add a summary of relevant changes into [release notes](https://github.com/akka/akka-sdk/blob/main/docs/src/modules/reference/pages/release-notes.adoc)

For minor or major releases:

- [ ] Include noteworthy features and improvements in Akka umbrella release announcement at akka.io. Coordinate with PM and marketing.

### Afterwards

- [ ] Update `MiMa.latestPatchOf` and PR that change (`project/MiMa.scala`)
- [ ] Update [akka-dependencies bom](https://github.com/lightbend/akka-dependencies) and version for [Akka module versions](https://doc.akka.io/libraries/akka-dependencies/current/) in [akka-dependencies repo](https://github.com/akka/akka-dependencies)
- [ ] Update [Akka Guide samples](https://github.com/akka/akka-guide)
- [ ] Update sbt new templates:
  - [ ] [Akka Scala](https://github.com/akka/akka-quickstart-scala.g8/blob/main/src/main/g8/default.properties)
  - [ ] [Akka HTTP Scala](https://github.com/akka/akka-http-quickstart-scala.g8/blob/main/src/main/g8/default.properties)
  - [ ] [Akka gRPC Scala](https://github.com/akka/akka-grpc-quickstart-scala.g8/blob/main/src/main/g8/default.properties)
- Close this issue
