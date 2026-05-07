---
project.description: Durable State with Akka Persistence enables actors to persist its state for recovery on failure or when migrated within a cluster.
---
# Durable State

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Persistence, add the module to your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-persistence-typed_$scala.binary.version$
  version=AkkaVersion
  group2=com.typesafe.akka
  artifact2=akka-persistence-testkit_$scala.binary.version$
  version2=AkkaVersion
  scope2=test
}

You also have to select durable state store plugin, see @ref:[Persistence Plugins](../../persistence-plugins.md).

@@project-info{ projectId="akka-persistence-typed" }

## Introduction

This model of Akka Persistence enables a stateful actor / entity to store the full state after processing each command instead of using event sourcing. This reduces the conceptual complexity and can be a handy tool for simple use cases. Very much like a CRUD based operation, the API is conceptually simple - a function from current state and incoming command to the next state which replaces the current state in the database. 

```
(State, Command) => State
```

The current state is always stored in the database. Since only the latest state is stored, we don't have access to any of the history of changes, unlike event sourced storage. Akka Persistence would read that state and store it in memory. After processing of the command is finished, the new state will be stored in the database. The processing of the next command will not start until the state has been successfully stored in the database.

Akka Persistence also supports @ref:[Event Sourcing](../persistence.md) based implementation, where only the _events_ that are persisted by the actor are stored, but not the actual state of the actor. By storing all events, using this model, 
a stateful actor can be recovered by replaying the stored events to the actor, which allows it to rebuild its state.

Since each entity lives on one node, consistency is guaranteed and reads can be served directly from memory. For details on how this guarantee
is ensured, have a look at the @ref:[Cluster Sharding and DurableStateBehavior](#cluster-sharding-and-durablestatebehavior) section below.

## Example and core API

Let's start with a simple example that models a counter using an Akka persistent actor. The minimum required for a @apidoc[DurableStateBehavior] is:

Scala
:  @@snip [DurableStatePersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/DurableStatePersistentBehaviorCompileOnly.scala) { #structure }

Java
:  @@snip [DurableStatePersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/DurableStatePersistentBehaviorTest.java) { #structure }

The first important thing to notice is the `Behavior` of a persistent actor is typed to the type of the `Command`
because this is the type of message a persistent actor should receive. In Akka this is now enforced by the type system.

The components that make up a `DurableStateBehavior` are:

* `persistenceId` is the stable unique identifier for the persistent actor.
* `emptyState` defines the `State` when the entity is first created e.g. a Counter would start with 0 as state.
* `commandHandler` defines how to handle commands and map to appropriate effects e.g. persisting state and replying to actors.

Next we'll discuss each of these in detail.

### PersistenceId

The @apidoc[akka.persistence.typed.PersistenceId] is the stable unique identifier for the persistent actor in the backend
durabe state store.

@ref:[Cluster Sharding](../cluster-sharding.md) is typically used together with `DurableStateBehavior` to ensure
that there is only one active entity for each `PersistenceId` (`entityId`). There are techniques to ensure this 
uniqueness, an example of which can be found in the 
@ref:[Persistence example in the Cluster Sharding documentation](../cluster-sharding.md#persistence-example). This illustrates how to construct the `PersistenceId` from the `entityTypeKey` and `entityId` provided by the `EntityContext`.

The `entityId` in Cluster Sharding is the business domain identifier which uniquely identifies the instance of
that specific `EntityType`. This means that across the cluster we have a unique combination of (`EntityType`, `EntityId`).
Hence the `entityId` might not be unique enough to be used as the `PersistenceId` by itself. For example 
two different types of entities may have the same `entityId`. To create a unique `PersistenceId` the `entityId` 
should be prefixed with a stable name of the entity type, which typically is the same as the `EntityTypeKey.name` that
is used in Cluster Sharding. There are @scala[`PersistenceId.apply`]@java[`PersistenceId.of`] factory methods
to help with constructing such `PersistenceId` from an `entityTypeHint` and `entityId`.

The default separator when concatenating the `entityTypeHint` and `entityId` is `|`, but a custom separator
is supported.

A custom identifier can be created with `PersistenceId.ofUniqueId`.  

### Command handler

The command handler is a function with 2 parameters, the current `State` and the incoming `Command`.

A command handler returns an `Effect` directive that defines what state, if any, to persist. 
Effects are created using @java[a factory that is returned via the `Effect()` method] @scala[the `Effect` factory].

The two most commonly used effects are: 

* `persist` will persist the latest value of the state. No history of state changes will be stored
* `none` no state to be persisted, for example a read-only command

The state is typically defined as an immutable class and a new instance of the state is passed to the `persist` effect.
You may choose to use a mutable class for the state, and then the command handler may update the state instance, but
it must still pass the updated state to the `persist` effect.

@@@ div { .group-java }

If the state is mutable, it is important that the `emptyState` method creates a new State instance each time
it is called to ensure that the state is recreated in case of failure restarts.

@@@

@@@ div { .group-scala }

If the state is mutable, it is important to use the `DurableStateBehavior.withMutableState` factory method that
takes `emptyStateFactory: () => State` parameter to make sure that the state instance is recreated in case of
failure restarts.

@@@

More effects are explained in @ref:[Effects and Side Effects](#effects-and-side-effects).

In addition to returning the primary `Effect` for the command, `DurableStateBehavior`s can also 
chain side effects that are to be performed after successful persist which is achieved with the `thenRun`
function e.g. @scala[`Effect.persist(..).thenRun`]@java[`Effect().persist(..).thenRun`].

### Completing the example

Let's fill in the details of the example.

Commands:

Scala
:  @@snip [DurableStatePersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/DurableStatePersistentBehaviorCompileOnly.scala) { #command }

Java
:  @@snip [DurableStatePersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/DurableStatePersistentBehaviorTest.java) { #command }

State is a storage for the latest value of the counter.

Scala
:  @@snip [DurableStatePersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/DurableStatePersistentBehaviorCompileOnly.scala) { #state }

Java
:  @@snip [DurableStatePersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/DurableStatePersistentBehaviorTest.java) { #state }

The command handler handles the commands `Increment`, `IncrementBy` and `GetValue`. 

* `Increment` increments the counter by `1` and persists the updated value as an effect in the State
* `IncrementBy` increments the counter by the value passed to it and persists the updated value as an effect in the State
* `GetValue` retrieves the value of the counter from the State and replies with it to the actor passed in

Scala
:  @@snip [DurableStatePersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/DurableStatePersistentBehaviorCompileOnly.scala) { #command-handler }

Java
:  @@snip [DurableStatePersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/DurableStatePersistentBehaviorTest.java) { #command-handler }

@scala[These are used to create a `DurableStateBehavior`:]
@java[These are defined in an `DurableStateBehavior`:]

Scala
:  @@snip [DurableStatePersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/DurableStatePersistentBehaviorCompileOnly.scala) { #behavior }

Java
:  @@snip [DurableStatePersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/DurableStatePersistentBehaviorTest.java) { #behavior }

## Effects and Side Effects

A command handler returns an `Effect` directive that defines what state, if any, to persist. 
Effects are created using @java[a factory that is returned via the `Effect()` method] @scala[the `Effect` factory]
and can be one of: 

* `persist` will persist the latest state. If it's a new persistence id, the record will be inserted. In case of an existing
persistence id, the record will be updated only if the revision number of the incoming record is 1 more than the already
existing record. Otherwise `persist` will fail.
* `delete` will delete the state by setting it to the empty state and the revision number will be incremented by 1.
* `none` no state to be persisted, for example a read-only command
* `unhandled` the command is unhandled (not supported) in current state
* `stop` stop this actor
* `stash` the current command is stashed
* `unstashAll` process the commands that were stashed with @scala[`Effect.stash`]@java[`Effect().stash`]
* `reply` send a reply message to the given `ActorRef`

Note that only one of those can be chosen per incoming command. It is not possible to both persist and say none/unhandled.

In addition to returning the primary `Effect` for the command `DurableStateBehavior`s can also 
chain side effects that are to be performed after successful persist which is achieved with the `thenRun`
function that runs the callback passed to it e.g. @scala[`Effect.persist(..).thenRun`]@java[`Effect().persist(..).thenRun`]. 

All `thenRun` registered callbacks are executed sequentially after successful execution of the persist statement
(or immediately, in case of `none` and `unhandled`).

In addition to `thenRun` the following actions can also be performed after successful persist:

* `thenStop` the actor will be stopped
* `thenUnstashAll` process the commands that were stashed with @scala[`Effect.stash`]@java[`Effect().stash`]
* `thenReply` send a reply message to the given `ActorRef`

In the example below, we use a different constructor of `DurableStateBehavior.withEnforcedReplies`, which creates
a `Behavior` for a persistent actor that ensures that every command sends a reply back. Hence it will be
a compilation error if the returned effect from a `CommandHandler` isn't a `ReplyEffect`.

Instead of `Increment` we will have a new command `IncrementWithConfirmation` that, along with persistence will also
send an acknowledgement as a reply to the `ActorRef` passed in the command. 

Example of effects and side-effects:

Scala
:  @@snip [DurableStatePersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/DurableStatePersistentBehaviorCompileOnly.scala) { #effects }

Java
:  @@snip [DurableStatePersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/DurableStatePersistentBehaviorTest.java) { #effects }

The most common way to have a side-effect is to use the `thenRun` method on `Effect`. In case you have multiple side-effects
that needs to be run for several commands, you can factor them out into functions and reuse for all the commands. For example:

Scala
:  @@snip [PersistentActorCompileOnlyTest.scala](/akka-persistence-typed/src/test/scala/akka/persistence/typed/scaladsl/PersistentActorCompileOnlyTest.scala) { #commonChainedEffects }

Java
:  @@snip [PersistentActorCompileOnlyTest.java](/akka-persistence-typed/src/test/java/akka/persistence/typed/javadsl/PersistentActorCompileOnlyTest.java) { #commonChainedEffects }

@@@ div { .group-scala }

It is recommended to use an immutable state class. If the state is mutable, it is important to use
the `DurableStateBehavior.withEnforcedRepliesMutableState` factory method that takes `emptyStateFactory: () => State`
parameter to make sure that the state instance is recreated in case of failure restarts.

@@@

### Side effects ordering and guarantees

Any side effects are executed on an at-most-once basis and will not be executed if the persist fails.

Side effects are not run when the actor is restarted or started again after being stopped.

The side effects are executed sequentially, it is not possible to execute side effects in parallel, unless they
call out to something that is running concurrently (for example sending a message to another actor).

It's possible to execute a side effect before persisting the state, but that can result in that the
side effect is performed but that the state is not stored if the persist fails.

## Cluster Sharding and DurableStateBehavior

@ref:[Cluster Sharding](../cluster-sharding.md) is an excellent fit to spread persistent actors over a
cluster, addressing them by id. It makes it possible to have more persistent actors exist in the cluster than what 
would fit in the memory of one node. Cluster sharding improves the resilience of the cluster. If a node crashes, 
the persistent actors are quickly started on a new node and can resume operations.

The `DurableStateBehavior` can then be run as any plain actor as described in @ref:[actors documentation](../actors.md),
but since Akka Persistence is based on the single-writer principle, the persistent actors are typically used together
with Cluster Sharding. For a particular `persistenceId` only one persistent actor instance should be active at one time.
Cluster Sharding ensures that there is only one active entity (or actor instance) for each id. 

## Accessing the ActorContext

If the @apidoc[DurableStateBehavior] needs to use the @apidoc[typed.*.ActorContext], for example to spawn child actors, it can be obtained by wrapping construction with `Behaviors.setup`:

Scala
:  @@snip [DurableStatePersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/DurableStatePersistentBehaviorCompileOnly.scala) { #actor-context }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/DurableStatePersistentBehaviorTest.java) { #actor-context }

## Changing Behavior

After processing a message, actors are able to return the `Behavior` that is used
for the next message.

As you can see in the above examples this is not supported by persistent actors. Instead, the state is
persisted as an `Effect` by the `commandHandler`. 

The reason a new behavior can't be returned is that behavior is part of the actor's
state and must also carefully be reconstructed during recovery from the persisted state. This would imply
that the state needs to be encoded such that the behavior can also be restored from it. 
That would be very prone to mistakes which is why it is not allowed in Akka Persistence.

For basic actors you can use the same set of command handlers independent of what state the entity is in.
For more complex actors it's useful to be able to change the behavior in the sense
that different functions for processing commands may be defined depending on what state the actor is in.
This is useful when implementing finite state machine (FSM) like entities.

The next example demonstrates how to define different behavior based on the current `State`. It shows an actor that
represents the state of a blog post. Before a post is started the only command it can process is to `AddPost`.
Once it is started then one can look it up with `GetPost`, modify it with `ChangeBody` or publish it with `Publish`.

The state is captured by:

Scala
:  @@snip [BlogPostEntityDurableState.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BlogPostEntityDurableState.scala) { #state }

Java
:  @@snip [BlogPostEntityDurableState.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BlogPostEntityDurableState.java) { #state }

The commands, of which only a subset are valid depending on the state:

Scala
:  @@snip [BlogPostEntityDurableState.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BlogPostEntityDurableState.scala) { #commands }

Java
:  @@snip [BlogPostEntityDurableState.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BlogPostEntityDurableState.java) { #commands }

@java[The command handler to process each command is decided by the state class (or state predicate) that is
given to the `forStateType` of the `CommandHandlerBuilder` and the match cases in the builders.]
@scala[The command handler to process each command is decided by first looking at the state and then the command.
It typically becomes two levels of pattern matching, first on the state and then on the command.]
Delegating to methods like `addPost`, `changeBody`, `publish` etc. is a good practice because the one-line cases give a nice overview of the message dispatch.

Scala
:  @@snip [BlogPostEntityDurableState.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BlogPostEntityDurableState.scala) { #command-handler }

Java
:  @@snip [BlogPostEntityDurableState.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BlogPostEntityDurableState.java) { #command-handler }

And finally the behavior is created @scala[from the `DurableStateBehavior.apply`]:

Scala
:  @@snip [BlogPostEntityDurableState.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BlogPostEntityDurableState.scala) { #behavior }

Java
:  @@snip [BlogPostEntityDurableState.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BlogPostEntityDurableState.java) { #behavior }

This can be refactored one or two steps further by defining the command handlers in the state class as
illustrated in @ref:[command handlers in the state](persistence-style.md#command-handlers-in-the-state).

There is also an example illustrating an @ref:[optional initial state](persistence-style.md#optional-initial-state).

## Replies

The @ref:[Request-Response interaction pattern](../interaction-patterns.md#request-response) is very common for
persistent actors, because you typically want to know if the command was rejected due to validation errors and
when accepted you want a confirmation when the events have been successfully stored.

Therefore you typically include a @scala[`ActorRef[ReplyMessageType]`]@java[`ActorRef<ReplyMessageType>`]. If the 
command can either have a successful response or a validation error returned, the generic response type @scala[`StatusReply[ReplyType]]`]
@java[`StatusReply<ReplyType>`] can be used. If the successful reply does not contain a value but is more of an acknowledgement
a pre defined @scala[`StatusReply.Ack`]@java[`StatusReply.ack()`] of type @scala[`StatusReply[Done]`]@java[`StatusReply<Done>`]
can be used.

After validation errors or after persisting events, using a `thenRun` side effect, the reply message can
be sent to the `ActorRef`.

Scala
:  @@snip [BlogPostEntityDurableState.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BlogPostEntityDurableState.scala) { #reply-command }

Java
:  @@snip [BlogPostEntityDurableState.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BlogPostEntityDurableState.java) { #reply-command }


Scala
:  @@snip [BlogPostEntityDurableState.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BlogPostEntityDurableState.scala) { #reply }

Java
:  @@snip [BlogPostEntityDurableState.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BlogPostEntityDurableState.java) { #reply }


Since this is such a common pattern there is a reply effect for this purpose. It has the nice property that
it can be used to enforce that you do not forget to specify replies when implementing the `DurableStateBehavior`.
If it's defined with @scala[`DurableStateBehavior.withEnforcedReplies`]@java[`DurableStateBehaviorWithEnforcedReplies`]
there will be compilation errors if the returned effect isn't a `ReplyEffect`, which can be
created with @scala[`Effect.reply`]@java[`Effect().reply`], @scala[`Effect.noReply`]@java[`Effect().noReply`],
@scala[`Effect.thenReply`]@java[`Effect().thenReply`], or @scala[`Effect.thenNoReply`]@java[`Effect().thenNoReply`].

Scala
:  @@snip [AccountExampleWithCommandHandlersInDurableState.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/AccountExampleWithCommandHandlersInDurableState.scala) { #withEnforcedReplies }

Java
:  @@snip [AccountExampleWithNullDurableState.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/AccountExampleWithNullDurableState.java) { #withEnforcedReplies }

The commands must have a field of @scala[`ActorRef[ReplyMessageType]`]@java[`ActorRef<ReplyMessageType>`] that can then be used to send a reply.

Scala
:  @@snip [AccountExampleWithCommandHandlersInDurableState.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/AccountExampleWithCommandHandlersInDurableState.scala) { #reply-command }

Java
:  @@snip [AccountExampleWithNullDurableState.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/AccountExampleWithNullDurableState.java) { #reply-command }

The `ReplyEffect` is created with @scala[`Effect.reply`]@java[`Effect().reply`], @scala[`Effect.noReply`]@java[`Effect().noReply`],
@scala[`Effect.thenReply`]@java[`Effect().thenReply`], or @scala[`Effect.thenNoReply`]@java[`Effect().thenNoReply`].

@java[Note that command handlers are defined with `newCommandHandlerWithReplyBuilder` when using
`EventSourcedBehaviorWithEnforcedReplies`, as opposed to newCommandHandlerBuilder when using `EventSourcedBehavior`.]

Scala
:  @@snip [AccountExampleWithCommandHandlersInDurableState.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/AccountExampleWithCommandHandlersInDurableState.scala) { #reply }

Java
:  @@snip [AccountExampleWithNullDurableState.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/AccountExampleWithNullDurableState.java) { #reply }

These effects will send the reply message even when @scala[`DurableStateBehavior.withEnforcedReplies`]@java[`DurableStateBehaviorWithEnforcedReplies`]
is not used, but then there will be no compilation errors if the reply decision is left out.

Note that the `noReply` is a way of making a conscious decision that a reply shouldn't be sent for a specific
command or that a reply will be sent later, perhaps after some asynchronous interaction with other actors or services.

## Serialization

The same @ref:[serialization](../../serialization.md) mechanism as for actor messages is also used for persistent actors.

You need to enable @ref:[serialization](../../serialization.md) for your commands (messages) and state.
@ref:[Serialization with Jackson](../../serialization-jackson.md) is a good choice in many cases and our
recommendation if you don't have other preference.

## Tagging

Persistence allows you to use tags in persistence query. Tagging allows you to identify a subset of states in the durable store
and separately consume them as a stream through the `DurableStateStoreQuery` interface. 

Scala
:  @@snip [DurableStatePersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/DurableStatePersistentBehaviorCompileOnly.scala) { #tagging }

Java
:  @@snip [DurableStatePersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/DurableStatePersistentBehaviorTest.java) { #tagging }

## Wrapping DurableStateBehavior

When creating a `DurableStateBehavior`, it is possible to wrap `DurableStateBehavior` in
other behaviors such as `Behaviors.setup` in order to access the `ActorContext` object. For instance
to access the logger from within the `ActorContext` to log for debugging the `commandHandler`.

Scala
:  @@snip [DurableStatePersistentActorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/DurableStatePersistentBehaviorCompileOnly.scala) { #wrapPersistentBehavior }

Java
:  @@snip [DurableStatePersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/DurableStatePersistentBehaviorTest.java) { #wrapPersistentBehavior }
