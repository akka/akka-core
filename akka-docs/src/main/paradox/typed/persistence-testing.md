# Testing

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Persistence TestKit, add the module to your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group1=com.typesafe.akka
  artifact1=akka-persistence-typed_$scala.binary.version$
  version1=AkkaVersion
  group2=com.typesafe.akka
  artifact2=akka-persistence-testkit_$scala.binary.version$
  version2=AkkaVersion
  scope2=test
}

@@project-info{ projectId="akka-persistence-testkit" }

## Unit testing with the BehaviorTestKit

**Note!** The `UnpersistentBehavior` is a new feature: the API may have changes breaking source compatibility in future versions.

Unit testing of `EventSourcedBehavior` can be performed by converting it into an @apidoc[UnpersistentBehavior]. Instead of
persisting events and snapshots, the `UnpersistentBehavior` exposes @apidoc[PersistenceProbe]s for events and snapshots which
can be asserted on.

Scala
: @@snip [AccountExampleUnpersistentDocSpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/AccountExampleUnpersistentDocSpec.scala) { #unpersistent-behavior }

Java
: @@snip [AccountExampleUnpersistentDocTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/AccountExampleUnpersistentDocTest.java) { #unpersistent-behavior }

The `UnpersistentBehavior` can be initialized with arbitrary states:

Scala
: @@snip [AccountExampleUnpersistentDocSpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/AccountExampleUnpersistentDocSpec.scala) { #unpersistent-behavior-provided-state }

Java
: @@snip [AccountExampleUnpersistentDocTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/AccountExampleUnpersistentDocTest.java) { #unpersistent-behavior-provided-state }

The `UnpersistentBehavior` is especially well-suited to the synchronous @ref:[`BehaviorTestKit`](testing-sync.md#synchronous-behavior-testing):
the `UnpersistentBehavior` can directly construct a `BehaviorTestKit` wrapping the behavior.  When commands are run by `BehaviorTestKit`,
they are processed in the calling thread (viz. the test suite), so when the run returns, the suite can be sure that the message has been
fully processed.  The internal state of the `EventSourcedBehavior` is not exposed to the suite except to the extent that it affects how
the behavior responds to commands or the events it persists (in addition, any snapshots made by the behavior are available through a
`PersistenceProbe`).

A full test for the `AccountEntity`, which is shown in the @ref:[Persistence Style Guide](persistence-style.md) might look like:

Scala
: @@snip [AccountExampleUnpersistentDocSpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/AccountExampleUnpersistentDocSpec.scala) { #test }

Java
: @@snip [AccountExampleUnpersistentDocTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/AccountExampleUnpersistentDocTest.java) { #test }

`UnpersistentBehavior` does not require any configuration.  It therefore does not verify the serialization of commands, events, or state.
If using this style, it is advised to independently test serialization for those classes.

## Unit testing with the the ActorTestKit and EventSourcedBehaviorTestKit

**Note!** The `EventSourcedBehaviorTestKit` is a new feature: the API may have changes breaking source compatibility in future versions.

Unit testing of `EventSourcedBehavior` can be done with the @apidoc[EventSourcedBehaviorTestKit]. It supports running
one command at a time and you can assert that the synchronously returned result is as expected. The result contains the
events emitted by the command and the new state after applying the events. It also has support for verifying the reply
to a command.

You need to configure the `ActorSystem` with the `EventSourcedBehaviorTestKit.config`. The configuration enables
the in-memory journal and snapshot storage.

Scala
:  @@snip [AccountExampleDocSpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/AccountExampleDocSpec.scala) { #testkit }

Java
:  @@snip [AccountExampleDocTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/AccountExampleDocTest.java) { #testkit } 

A full test for the `AccountEntity`, which is shown in the @ref:[Persistence Style Guide](persistence-style.md), may look like this:

Scala
:  @@snip [AccountExampleDocSpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/AccountExampleDocSpec.scala) { #test }

Java
:  @@snip [AccountExampleDocTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/AccountExampleDocTest.java) { #test }  

Serialization of commands, events and state are verified automatically. The serialization checks can be
customized with the `SerializationSettings` when creating the `EventSourcedBehaviorTestKit`. By default,
the serialization roundtrip is checked but the equality of the result of the serialization is not checked.
`equals` must be implemented @scala[(or using `case class`)] in the commands, events and state if `verifyEquality`
is enabled.

To test recovery the `restart` method of the `EventSourcedBehaviorTestKit` can be used. It will restart the
behavior, which will then recover from stored snapshot and events from previous commands. It's also possible
to populate the storage with events or simulate failures by using the underlying @apidoc[PersistenceTestKit].

## Persistence TestKit

**Note!** The `PersistenceTestKit` is a new feature: the API may have changes breaking source compatibility in future versions.

Persistence testkit allows to check events saved in a storage, emulate storage operations and exceptions.
To use the testkit you need to add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-persistence-testkit_$scala.binary.version$"
  version=AkkaVersion
}

There are two testkit classes which have similar api:

 * @apidoc[PersistenceTestKit] class is for events
 * @apidoc[SnapshotTestKit] class is for snapshots
 
The testkit classes have two corresponding plugins which emulate the behavior of the storages: 

 * @apidoc[PersistenceTestKitPlugin] class emulates a events storage 
 * @apidoc[PersistenceTestKitSnapshotPlugin] class emulates a snapshots storage

**Note!** The corresponding plugins **must** be configured in the actor system which is used to initialize the particular testkit class:

Scala
:  @@snip [Configuration.scala](/akka-docs/src/test/scala/docs/persistence/testkit/Configuration.scala) { #testkit-typed-conf }

Java
:  @@snip [Configuration.java](/akka-docs/src/test/java/jdocs/persistence/testkit/Configuration.java) { #testkit-typed-conf }

and

Scala
:  @@snip [Configuration.scala](/akka-docs/src/test/scala/docs/persistence/testkit/Configuration.scala) { #snapshot-typed-conf }

Java
:  @@snip [Configuration.java](/akka-docs/src/test/java/jdocs/persistence/testkit/Configuration.java) { #snapshot-typed-conf }

A typical scenario is to create a persistent actor, send commands to it and check that it persists events as it is expected:

Scala
:  @@snip [TestKitExamples.scala](/akka-docs/src/test/scala/docs/persistence/testkit/TestKitExamples.scala) { #test }

Java
:  @@snip [PersistenceTestKitSampleTest.java](/akka-docs/src/test/java/jdocs/persistence/testkit/PersistenceTestKitSampleTest.java) { #test }

You can safely use persistence testkit in combination with main akka testkit.

The main methods of the api allow to (see @apidoc[PersistenceTestKit] and @apidoc[SnapshotTestKit] for more details):

 * check if the given event/snapshot object is the next persisted in the storage.
 * read a sequence of persisted events/snapshots.
 * check that no events/snapshots have been persisted in the storage.
 * throw the default exception from the storage on attempt to persist, read or delete the following event/snapshot.
 * clear the events/snapshots persisted in the storage.
 * reject the events, but not snapshots (rejections are not supported for snapshots in the original api).
 * set your own [policy](#setting-your-own-policy-for-the-storage) which emulates the work of the storage. 
Policy determines what to do when persistence needs to execute some operation on the storage (i.e. read, delete, etc.).
 * get all the events/snapshots persisted in the storage
 * put the events/snapshots in the storage to test recovery
 
#### Setting your own policy for the storage

You can implement and set your own policy for the storage to control its actions on particular operations, for example you can fail or reject events on your own conditions.
Implement the @apidoc[ProcessingPolicy[EventStorage.JournalOperation]] @scala[trait]@java[interface] for event storage
or @apidoc[ProcessingPolicy[SnapshotStorage.SnapshotOperation]] @scala[trait]@java[interface] for snapshot storage,
and set it with `withPolicy()` method.

Scala
:  @@snip [TestKitExamples.scala](/akka-docs/src/test/scala/docs/persistence/testkit/TestKitExamples.scala) { #policy-test }

Java
:  @@snip [PersistenceTestKitPolicySampleTest.java](/akka-docs/src/test/java/jdocs/persistence/testkit/PersistenceTestKitPolicySampleTest.java) { #policy-test }

`tryProcess()` method of the @apidoc[ProcessingPolicy] has two arguments: persistence id and the storage operation. 

Event storage has the following operations:

 * @apidoc[ReadEvents] Read the events from the storage.
 * @apidoc[WriteEvents] Write the events to the storage.
 * @apidoc[DeleteEvents] Delete the events from the storage.
 * @apidoc[ReadSeqNum$] Read the highest sequence number for particular persistence id.

Snapshot storage has the following operations:

 * @apidoc[ReadSnapshot] Read the snapshot from the storage.
 * @apidoc[WriteSnapshot] Writhe the snapshot to the storage.
 * @apidoc[DeleteSnapshotsByCriteria] Delete snapshots in the storage by criteria.
 * @apidoc[DeleteSnapshotByMeta] Delete particular snapshot from the storage by its metadata.

The `tryProcess()` method must return one of the processing results:
 
 * @apidoc[ProcessingSuccess] Successful completion of the operation. All the events will be saved/read/deleted.
 * @apidoc[StorageFailure] Emulates exception from the storage.
 * @apidoc[Reject] Emulates rejection from the storage.

**Note** that snapshot storage does not have rejections. If you return `Reject` in the `tryProcess()` of the snapshot storage policy, it will have the same effect as the `StorageFailure`.

Here is an example of the policy for an event storage:

Scala
:  @@snip [TestKitExamples.scala](/akka-docs/src/test/scala/docs/persistence/testkit/TestKitExamples.scala) { #set-event-storage-policy }

Java
:  @@snip [TestKitExamples.java](/akka-docs/src/test/java/jdocs/persistence/testkit/TestKitExamples.java) { #set-event-storage-policy }

Here is an example of the policy for a snapshot storage:

Scala
:  @@snip [TestKitExamples.scala](/akka-docs/src/test/scala/docs/persistence/testkit/TestKitExamples.scala) { #set-snapshot-storage-policy }

Java
:  @@snip [TestKitExamples.java](/akka-docs/src/test/java/jdocs/persistence/testkit/TestKitExamples.java) { #set-snapshot-storage-policy } 

### Configuration of Persistence TestKit

There are several configuration properties for persistence testkit, please refer
to the @ref:[reference configuration](../general/configuration-reference.md#config-akka-persistence-testkit)

## Integration testing

`EventSourcedBehavior` actors can be tested with the @ref:[ActorTestKit](testing-async.md) together with
other actors. The in-memory journal and snapshot storage from the @ref:[Persistence TestKit](#persistence-testkit)
can be used also for integration style testing of a single `ActorSystem`, for example when using Cluster Sharding
with a single Cluster node.

For tests that involve more than one Cluster node you have to use another journal and snapshot store.
While it's possible to use the @ref:[Persistence Plugin Proxy](../persistence-plugins.md#persistence-plugin-proxy)
it's often better and more realistic to use a real database.

The @extref[Microservices with Akka tutorial](platform-guide:microservices-tutorial/) includes tests that are using a real database.

### Plugin initialization

Some Persistence plugins create tables automatically, but has the limitation that it can't be done concurrently
from several ActorSystems. That can be a problem if the test creates a Cluster and all nodes tries to initialize
the plugins at the same time. To coordinate initialization you can use the `PersistenceInit` utility.

`PersistenceInit` is part of `akka-persistence-testkit` and you need to add the dependency to your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-persistence-testkit_$scala.binary.version$"
  version=AkkaVersion
}

Scala
:  @@snip [PersistenceInitSpec.scala](/akka-docs/src/test/scala/docs/persistence/testkit/PersistenceInitSpec.scala) { #imports #init }

Java
:  @@snip [PersistenceInitTest.java](/akka-docs/src/test/java/jdocs/persistence/testkit/PersistenceInitTest.java) { #imports #init }
