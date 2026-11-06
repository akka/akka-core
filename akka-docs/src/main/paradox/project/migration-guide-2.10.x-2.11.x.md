---
project.description: Migrating to Akka 2.11.
---
# Migration Guide 2.10.x to 2.11.x

## Jackson 2.21

@ref:[Akka Serialization Jackson](../serialization-jackson.md) now depends on the Jackson 2.21 long-term-support line. Akka 2.10 used Jackson 2.18 which will not get further updates past January 2027.

## Removed deprecated APIs

APIs deprecated in Akka 2.10.0 or earlier have been removed. The sections below list the removed APIs and their replacements.

### akka-testkit

* `akka.testkit.javadsl.TestKit` overloads of `expectMsgPF`, `expectTerminated`, `fishForMessage` and `fishForSpecificMessage` taking a `scala.concurrent.duration.Duration`: use the overloads that accept `java.time.Duration` instead.
