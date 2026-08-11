Release Akka $VERSION$

<!--
# Release Train Issue Template for Akka

(Liberally copied and adopted from Scala itself https://github.com/scala/scala-dev/blob/b11cd2e4a4431de7867db6b39362bea8fa6650e7/notes/releases/template.md)

For every release, use the `scripts/create-release-issue.sh` to make a copy of this file named after the release, and expand the variables.

Variables to be expanded in this template:
- $VERSION$=???

Key links:
  - akka/akka milestone: https://github.com/akka/akka/milestone/?
-->

### Cutting the release

- [ ] Check that open PRs and issues assigned to the milestone are reasonable
- [ ] If PRs were merged after EU midnight, trigger the [native-image tests](https://github.com/akka/akka/actions/workflows/native-image-tests.yml) and see that they are green.
- [ ] Update the version and change date in the LICENSE file.
- [ ] Update the Akka version in the samples to $VERSION$, otherwise the published zip files of the samples will have the old version.
- [ ] Create a new milestone for the [next version](https://github.com/akka/akka/milestones)
- [ ] Close the [$VERSION$ milestone](https://github.com/akka/akka/milestones?direction=asc&sort=due_date)
- [ ] Make sure all important PRs have been merged
- [ ] Update the revision in Fossa in the Akka Group for the Akka umbrella version, e.g. `22.10`. Note that the revisions for the release is udpated by Akka Group > Projects > Edit. For recent dependency updates the Fossa validation can be triggered from the GitHub actions "Dependency License Scanning".
- [ ] Wait until [main build finished](https://github.com/akka/akka/actions) after merging the latest PR
- [ ] Update the [draft release](https://github.com/akka/akka/releases) with the next tag version `v$VERSION$`, title and release description. Use the `Publish release` button, which will create the tag.
- [ ] Check that GitHub Actions release build has executed successfully (GitHub Actions will start a [CI build](https://github.com/akka/akka/actions) for the new tag and publish artifacts to https://repo.akka.io/TOKEN/secure)

### Check availability

- [ ] Check [API](https://doc.akka.io/api/akka/$VERSION$/) documentation
- [ ] Check [reference](https://doc.akka.io/docs/akka/$VERSION$/) documentation. Check that the reference docs were deployed and show a version warning (see section below on how to fix the version warning).
- [ ] Check the release on https://repo.akka.io/maven/com/typesafe/akka/akka-actor_2.13/$VERSION$/akka-actor_2.13-$VERSION$.pom

### When everything is on https://repo.akka.io/TOKEN/secure
  - [ ] Log into `gustav.akka.io` as `akkarepo` 
    - [ ] If this updates the `current` version, run `./update-akka-current-version.sh $VERSION$`
    - [ ] otherwise check changes and commit the new version to the local git repository
         ```
         cd ~/www
         git status
         git add docs/akka/current docs/akka/$VERSION$
         git add api/akka/current api/akka/$VERSION$
         git add japi/akka/current japi/akka/$VERSION$
         git commit -m "Akka $VERSION$"
         ```
    - [ ] push changes to the [remote git repository](https://github.com/akka/doc.akka.io)
         ```
         cd ~/www
         git push origin master
         ```
  - [ ] If this updated 'current' docs - trigger a re-index of the docs for search through [Run workflow for the scraper](https://github.com/akka/akka/actions/workflows/algolia-doc-site-scrape.yml)
  - [ ] Update version in _config.yml in https://github.com/akka/akka.io    
  

### Announcements

For important patch releases, and only if critical issues have been fixed:

- [ ] Send a release notification to [Lightbend discuss](https://discuss.akka.io)
- [ ] Tweet using the [@akkateam](https://twitter.com/akkateam/) account (or ask someone to) about the new release
- [ ] Announce internally (with links to Tweet, discuss)

For minor or major releases:

- [ ] Include noteworthy features and improvements in Akka umbrella release announcement at akka.io. Coordinate with PM and marketing.

### Afterwards

- [ ] Update `MiMa.latestPatchOf` and PR that change (`project/MiMa.scala`)
- [ ] Update [akka-dependencies bom](https://github.com/lightbend/akka-dependencies) and version for [Akka module versions](https://doc.akka.io/docs/akka-dependencies/current/) in [akka-dependencies repo](https://github.com/akka/akka-dependencies)
- [ ] Update [Akka Guide samples](https://github.com/akka/akka-guide)
- [ ] Update sbt new templates:
  - [ ] [Akka Scala](https://github.com/akka/akka-quickstart-scala.g8/blob/main/src/main/g8/default.properties)
  - [ ] [Akka HTTP Scala](https://github.com/akka/akka-http-quickstart-scala.g8/blob/main/src/main/g8/default.properties)
  - [ ] [Akka gRPC Scala](https://github.com/akka/akka-grpc-quickstart-scala.g8/blob/main/src/main/g8/default.properties)
- Close this issue
