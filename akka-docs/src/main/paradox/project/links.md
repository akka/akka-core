# Project

## Commercial Support

Commercial support is provided by [Lightbend](https://www.lightbend.com).

## Sponsors

**Lightbend** is the company behind the Akka Project, Scala Programming Language,
Play Web Framework, Lagom, sbt and many other open source and source available projects. 
It also provides the Lightbend Reactive Platform, which is powered by a source available core and commercial Enterprise Suite for building scalable Reactive systems on the JVM. Learn more at [lightbend.com](https://www.lightbend.com).

## Akka Discuss Forums

[Akka Discuss Forums](https://discuss.akka.io)

## Source Code

Akka uses Git and is hosted at [Github akka/akka](https://github.com/akka/akka).

## Releases Repository

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

## Snapshots Repository

Snapshot builds are available from the repository at `https://repo.akka.io/TOKEN/secure/snapshots`. All Akka modules that belong to the same build have the same version.

@@@ warning

The use of Akka SNAPSHOTs, nightlies and milestone releases is discouraged unless you know what you are doing.

@@@

### sbt definition of snapshot repository

Make sure that you add the repository to the sbt resolvers:

```
resolvers += "Akka library snapshot repository".at("https://repo.akka.io/TOKEN/secure/snapshots")
```

Define the library dependencies with the complete version. For example:

@@@vars
```
libraryDependencies += "com.typesafe.akka" % "akka-cluster_$scala.binary.version$" % "2.9.0+72-53943d99-SNAPSHOT"
```
@@@

### Maven definition of snapshot repository

Make sure that you add the repository to the Maven repositories in pom.xml:

```
<repositories>
  <repositories>
    <repository>
      <id>akka-repository</id>
      <name>Akka library snapshot repository</name>
      <url>https://repo.akka.io/TOKEN/secure/snapshots</url>
    </repository>
  </repositories>
</repositories>
```

Define the library dependencies with the complete version. For example:

@@@vars
```
<dependencies>
  <dependency>
    <groupId>com.typesafe.akka</groupId>
    <artifactId>akka-cluster_$scala.binary.version$</artifactId>
    <version>2.9.0+72-53943d99-SNAPSHOT</version>
  </dependency>
</dependencies>
```
@@@
