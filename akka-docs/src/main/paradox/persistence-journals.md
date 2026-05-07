# Persistence - Building a storage backend 

Storage backends for journals and snapshot stores are pluggable in the Akka persistence extension.
A directory of persistence journal and snapshot store plugins is available at the Akka Community Projects page, see [Community plugins](https://akka.io/community/)
This documentation described how to build a new storage backend.

Applications can provide their own plugins by implementing a plugin API and activating them by configuration.
Plugin development requires the following imports:

Scala
:  @@snip [PersistencePluginDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistencePluginDocSpec.scala) { #plugin-imports }

Java
:  @@snip [LambdaPersistencePluginDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistencePluginDocTest.java) { #plugin-imports }

## Journal plugin API

A journal plugin extends `AsyncWriteJournal`.

`AsyncWriteJournal` is an actor and the methods to be implemented are:

Scala
:  @@snip [AsyncWriteJournal.scala](/akka-persistence/src/main/scala/akka/persistence/journal/AsyncWriteJournal.scala) { #journal-plugin-api }

Java
:  @@snip [AsyncWritePlugin.java](/akka-persistence/src/main/java/akka/persistence/journal/japi/AsyncWritePlugin.java) { #async-write-plugin-api }

If the storage backend API only supports synchronous, blocking writes, the methods should be implemented as:

Scala
:  @@snip [PersistencePluginDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistencePluginDocSpec.scala) { #sync-journal-plugin-api }

Java
:  @@snip [LambdaPersistencePluginDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistencePluginDocTest.java) { #sync-journal-plugin-api }

A journal plugin must also implement the methods defined in `AsyncRecovery` for replays and sequence number recovery:

Scala
:  @@snip [AsyncRecovery.scala](/akka-persistence/src/main/scala/akka/persistence/journal/AsyncRecovery.scala) { #journal-plugin-api }

Java
:  @@snip [AsyncRecoveryPlugin.java](/akka-persistence/src/main/java/akka/persistence/journal/japi/AsyncRecoveryPlugin.java) { #async-replay-plugin-api }

A journal plugin can be activated with the following minimal configuration:

@@snip [PersistencePluginDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistencePluginDocSpec.scala) { #journal-plugin-config }

The journal plugin instance is an actor so the methods corresponding to requests from persistent actors
are executed sequentially. It may delegate to asynchronous libraries, spawn futures, or delegate to other
actors to achieve parallelism.

The journal plugin class must have a constructor with one of these signatures:

 * constructor with one `com.typesafe.config.Config` parameter and a `String` parameter for the config path
 * constructor with one `com.typesafe.config.Config` parameter
 * constructor without parameters

The plugin section of the actor system's config will be passed in the config constructor parameter. The config path
of the plugin is passed in the `String` parameter.

The `plugin-dispatcher` is the dispatcher used for the plugin actor. If not specified, it defaults to
`akka.actor.default-dispatcher`.

Don't run journal tasks/futures on the system default dispatcher, since that might starve other tasks.

## Snapshot store plugin API

A snapshot store plugin must extend the `SnapshotStore` actor and implement the following methods:

Scala
:  @@snip [SnapshotStore.scala](/akka-persistence/src/main/scala/akka/persistence/snapshot/SnapshotStore.scala) { #snapshot-store-plugin-api }

Java
:  @@snip [SnapshotStorePlugin.java](/akka-persistence/src/main/java/akka/persistence/snapshot/japi/SnapshotStorePlugin.java) { #snapshot-store-plugin-api }

A snapshot store plugin can be activated with the following minimal configuration:

@@snip [PersistencePluginDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistencePluginDocSpec.scala) { #snapshot-store-plugin-config }

The snapshot store instance is an actor so the methods corresponding to requests from persistent actors
are executed sequentially. It may delegate to asynchronous libraries, spawn futures, or delegate to other
actors to achieve parallelism.

The snapshot store plugin class must have a constructor with one of these signatures:

 * constructor with one `com.typesafe.config.Config` parameter and a `String` parameter for the config path
 * constructor with one `com.typesafe.config.Config` parameter
 * constructor without parameters

The plugin section of the actor system's config will be passed in the config constructor parameter. The config path
of the plugin is passed in the `String` parameter.

The `plugin-dispatcher` is the dispatcher used for the plugin actor. If not specified, it defaults to
`akka.actor.default-dispatcher`.

Don't run snapshot store tasks/futures on the system default dispatcher, since that might starve other tasks.

## Plugin TCK

In order to help developers build correct and high quality storage plugins, we provide a Technology Compatibility Kit ([TCK](https://en.wikipedia.org/wiki/Technology_Compatibility_Kit) for short).

The TCK is usable from Java as well as Scala projects. To test your implementation (independently of language) you 
need to include the akka-persistence-tck dependency.

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-persistence-tck_$scala.binary.version$"
  version=AkkaVersion
}

To include the Journal TCK tests in your test suite simply extend the provided @scala[`JournalSpec`]@java[`JavaJournalSpec`]:

Scala
:  @@snip [PersistencePluginDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistencePluginDocSpec.scala) { #journal-tck-scala }

Java
:  @@snip [LambdaPersistencePluginDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistencePluginDocTest.java) { #journal-tck-java }

Please note that some of the tests are optional, and by overriding the `supports...` methods you give the
TCK the needed information about which tests to run. You can implement these methods using @scala[boolean values or] the
provided `CapabilityFlag.on` / `CapabilityFlag.off` values.

We also provide a simple benchmarking class @scala[`JournalPerfSpec`]@java[`JavaJournalPerfSpec`] which includes all the tests that @scala[`JournalSpec`]@java[`JavaJournalSpec`]
has, and also performs some longer operations on the Journal while printing its performance stats. While it is NOT aimed
to provide a proper benchmarking environment it can be used to get a rough feel about your journal's performance in the most
typical scenarios.

In order to include the `SnapshotStore` TCK tests in your test suite extend the `SnapshotStoreSpec`:

Scala
:  @@snip [PersistencePluginDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistencePluginDocSpec.scala) { #snapshot-store-tck-scala }

Java
:  @@snip [LambdaPersistencePluginDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistencePluginDocTest.java) { #snapshot-store-tck-java }

In case your plugin requires some setting up (starting a mock database, removing temporary files etc.) you can override the
`beforeAll` and `afterAll` methods to hook into the tests lifecycle:

Scala
:  @@snip [PersistencePluginDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistencePluginDocSpec.scala) { #journal-tck-before-after-scala }

Java
:  @@snip [LambdaPersistencePluginDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistencePluginDocTest.java) { #journal-tck-before-after-java }

We *highly recommend* including these specifications in your test suite, as they cover a broad range of cases you
might have otherwise forgotten to test for when writing a plugin from scratch.

## Corrupt event logs

If a journal can't prevent users from running persistent actors with the same `persistenceId` concurrently it is likely that an event log
will be corrupted by having events with the same sequence number.

It is recommended that journals should still deliver these events during recovery so that a `replay-filter` can be used to decide what to do about it
in a journal agnostic way.



