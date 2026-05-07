# Releasing

Create a new issue from the [Release Train Issue Template](akka-docs/release-train-issue-template.md):

```
$ sh ./scripts/create-release-issue.sh 0.x.y
```

# Manually

## Prerequisites

### One Time GPG and sbt-pgp setup

If you have not set up GPG or used `sbt-pgp` on the release machine
* Check the [sbt-pgp usage](https://www.scala-sbt.org/sbt-pgp/usage.html) for any setup steps you may still need, for example:
```
sbt> set pgpReadOnly := false
sbt> pgp-cmd gen-key
```    
* Check that signing works with `sbt> publishLocalSigned`
   
#### Mac

When releasing from MacOS you may want to use YubiKey or have [MacGPG](https://gpgtools.org) installed.

#### Windows

When releasing from Windows, you need MinGW and a gpg distribution such as Gpg4Win

### Git

Make sure you have set `core.autocrlf` to `false` in your `~/.gitconfig`,
otherwise git might convert line endings in some cases.

### Install Graphviz

[Graphvis](https://graphviz.gitlab.io/download/) is needed for the 
scaladoc generation build task, which is part of the release.
 
## Snapshot releases

Snapshot releases are created from `main` and published to https://repo.akka.io/TOKEN/secure/snapshots/

To create snapshot versions manually, use `sbt clean publishLocal`.
If you have access, you can use `+publishSigned` to publish them to
repo.akka.io.

## Releasing only updated docs

It is possible to release a revised documentation to the already existing release.

1. Create a new branch from a release tag. If a revised documentation is for the `v2.6.4` release, then the name of the new branch should be `docs/v2.6.4`:
    ```
    $ git checkout v2.6.4
    $ git checkout -b docs/v2.6.4
    ```
1. Add and commit `version.sbt` file that pins the version to the one that is being revised. Also set `isSnapshot` to `false` for the stable documentation links. For example:
    ```scala
    ThisBuild / version := "2.6.4"
    ThisBuild / isSnapshot := false
    ```
1. Switch to a new branch for your documentation change, make the change
1. Build documentation locally with:
    ```sh
    sbt akka-docs/paradoxBrowse
    ```
1. If the generated documentation looks good, create a PR to the `docs/v2.6.4` branch you created earlier.
1. It should automatically be published by GitHub Actions on merge.
