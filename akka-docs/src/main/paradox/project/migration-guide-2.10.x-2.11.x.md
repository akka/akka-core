---
project.description: Migrating to Akka 2.11.
---
# Migration Guide 2.10.x to 2.11.x

## Jackson 2.21

@ref:[Akka Serialization Jackson](../serialization-jackson.md) now depends on the Jackson 2.21 long-term-support line. Akka 2.10 used Jackson 2.18 which will not get further updates past January 2027.

## Removed deprecated APIs

APIs deprecated in Akka 2.10.0 or earlier have been removed. The sections below list the removed APIs and their replacements.

### akka-actor

* `FSM.setTimer`, `AbstractFSM.setTimer` and `TestFSMRef.setTimer`: use `startSingleTimer`, `startTimerWithFixedDelay` or `startTimerAtFixedRate`.
* `TimerScheduler.startPeriodicTimer`: use `startTimerWithFixedDelay` or `startTimerAtFixedRate`.
* The `Scheduler.schedule` overloads for repeated scheduling, including the `AbstractScheduler` variant for Java: use `scheduleWithFixedDelay` or `scheduleAtFixedRate`.
* `Props.create(Creator)` without an actor class: use `Props.create(actorClass, creator)`.
* `ScalaActorRef` and the implicit conversions in the `akka.actor` package object: `!` and `tell` are defined on `ActorRef` itself, so no conversion is needed.
* The `java.util.logging` integration in `akka.event.jul` (`JavaLogger`, `JavaLogging` and `JavaLoggingFilter`): use SLF4J with `akka.event.slf4j.Slf4jLogger` and `Slf4jLoggingFilter`, see @ref:[Logging](../logging.md).
* The DNS messages `Dns.Resolve` and `Dns.Resolved`, and `Dns.cached(name)` and `Dns.resolve(name)`: use `DnsProtocol.Resolve` and `DnsProtocol.Resolved`, with `Dns.cached(DnsProtocol.Resolve(name))` and `Dns.resolve(DnsProtocol.Resolve(name))`, see @ref:[DNS Extension](../io-dns.md).
* `Tcp.WriteFile`: use `Tcp.WritePath`.
* `Serialization.deserialize(bytes, serializerId, Option[Class])`: use the overload that takes the manifest as a class name.
* `ActorSystem.Settings.EnableAdditionalSerializationBindings`: the additional serialization bindings are always enabled since Akka 2.6.
