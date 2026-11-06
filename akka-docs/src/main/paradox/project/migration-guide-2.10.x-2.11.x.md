---
project.description: Migrating to Akka 2.11.
---
# Migration Guide 2.10.x to 2.11.x

## Jackson 2.21

@ref:[Akka Serialization Jackson](../serialization-jackson.md) now depends on the Jackson 2.21 long-term-support line. Akka 2.10 used Jackson 2.18 which will not get further updates past January 2027.

## Removed deprecated APIs

APIs deprecated in Akka 2.9 or earlier, or in Akka 2.10.0, have been removed. The sections below list the removed APIs and their replacements.

### akka-actor-typed

* `akka.actor.typed.scaladsl.LoggerOps`: not needed in Scala 2.13 and later, use the `org.slf4j.Logger` methods directly.
* `TimerScheduler.startPeriodicTimer` (scaladsl and javadsl): use `startTimerWithFixedDelay` or `startTimerAtFixedRate` instead. `startPeriodicTimer` had the same semantics as `startTimerAtFixedRate`, but `startTimerWithFixedDelay` is often preferred.
