---
project.description: Migrating to Akka 2.11.
---
# Migration Guide 2.10.x to 2.11.x

## Jackson 2.21

@ref:[Akka Serialization Jackson](../serialization-jackson.md) now depends on the Jackson 2.21 long-term-support line. Akka 2.10 used Jackson 2.18 which will not get further updates past January 2027.

## Removed deprecated APIs

APIs deprecated in Akka 2.10.0 or earlier have been removed. The sections below list the removed APIs and their replacements.

### akka-persistence

* The LevelDB journal (`akka.persistence.journal.leveldb`, `LeveldbJournal`) and the shared LevelDB journal (`akka.persistence.journal.leveldb-shared`, `SharedLeveldbJournal`, `SharedLeveldbStore`): use another journal implementation, such as [Akka Persistence R2DBC](https://doc.akka.io/libraries/akka-persistence-r2dbc/current/). For tests, use the `akka.persistence.journal.inmem` journal, in combination with the @ref:[journal proxy](../persistence-plugins.md#persistence-plugin-proxy) when several actor systems share one journal. The `akka.persistence.journal.leveldb` and `akka.persistence.journal.leveldb-shared` configuration sections no longer exist.
* The LevelDB read journal (`akka.persistence.query.journal.leveldb`, `LeveldbReadJournal`, `LeveldbReadJournalProvider`) in akka-persistence-query is removed with the LevelDB journal.
* `DurableStateUpdateStore.deleteObject(persistenceId)` in `akka.persistence.state.scaladsl` and `akka.persistence.state.javadsl`: use the `deleteObject` overload with `revision` instead. Durable state store plugins must remove their implementation of the overload without `revision`.
