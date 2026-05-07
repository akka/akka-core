---
project.description: Migrating to Akka 2.9.
---
# Migration Guide 2.9.x to 2.10.x

## Support for slf4j 1.7.x and logback 1.2.x removed

This is the first release that only supports slf4j 2.0.x and logback 1.5.x.

## Changes to Java Flight Recorder (JFR)

The config `akka.java-flight-recorder.enabled` was removed. Instead, the JFR interactions are recorded directly.

Please use the jvm native flags to take advantage of the JFR recorded events.

## Remove dependency to scala-java8-compat

The transitive dependency on `scala-java8-compat` has been removed.

## Deprecation of `akka.actor.typed.scaladsl.LoggerOps`

All helper methods like `LoggerOps.info2` can be replaced by using the underlying `org.slf4j.Logger` apis.

## Deprecation of Multi-DC Cluster

Use @extref[Akka Distributed Cluster](akka-distributed-cluster:) and @extref[Akka Edge](akka-edge:) instead.

