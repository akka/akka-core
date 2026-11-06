---
project.description: Migrating to Akka 2.11.
---
# Migration Guide 2.10.x to 2.11.x

## Jackson 2.21

@ref:[Akka Serialization Jackson](../serialization-jackson.md) now depends on the Jackson 2.21 long-term-support line. Akka 2.10 used Jackson 2.18 which will not get further updates past January 2027.

## Removed deprecated APIs

APIs deprecated in Akka 2.9 or earlier, or in Akka 2.10.0, have been removed. The sections below list the removed APIs and their replacements.

### akka-remote

* `AddressUidExtension`: use `ExtendedActorSystem.uid` instead.
* `akka.remote.serialization.LongSerializer`, `IntSerializer`, `StringSerializer` and `ByteStringSerializer`: moved to `akka.serialization` in akka-actor. The `akka.actor.serialization-identifiers` entries for the old class names have been removed; the serializer ids 18 to 21 are unchanged and still bound to the akka-actor serializers.
