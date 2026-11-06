---
project.description: Migrating to Akka 2.11.
---
# Migration Guide 2.10.x to 2.11.x

## Jackson 2.21

@ref:[Akka Serialization Jackson](../serialization-jackson.md) now depends on the Jackson 2.21 long-term-support line. Akka 2.10 used Jackson 2.18 which will not get further updates past January 2027.

## Removed deprecated APIs

APIs deprecated in Akka 2.9 or earlier, or in Akka 2.10.0, have been removed. The sections below list the removed APIs and their replacements.

### akka-persistence-query

* The LevelDB read journal (`akka.persistence.query.journal.leveldb`, `LeveldbReadJournal` in scaladsl and javadsl, `LeveldbReadJournalProvider`): use another journal and query implementation, such as [Akka Persistence R2DBC](https://doc.akka.io/libraries/akka-persistence-r2dbc/current/). The `akka.persistence.query.journal.leveldb` configuration section no longer exists.
* `EventEnvelope(offset, persistenceId, sequenceNr, event)` and `new EventEnvelope(offset, persistenceId, sequenceNr, event)` in `akka.persistence.query`: use the overloads with a `timestamp` parameter. The `EventEnvelope` companion object no longer extends `Function4`.
