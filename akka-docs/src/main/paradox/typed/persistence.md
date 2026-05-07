---
project.description: Event Sourcing with Akka Persistence enables actors to persist your events for recovery on failure or when migrated within a cluster.
---
# Event Sourcing

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Akka Persistence](../persistence.md).

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

You also have to select journal plugin and optionally snapshot store plugin, see 
@ref:[Persistence Plugins](../persistence-plugins.md).

@@project-info{ projectId="akka-persistence-typed" }

## Introduction

Akka Persistence enables stateful actors to persist their state so that it can be recovered when an actor
is either restarted, such as after a JVM crash, by a supervisor or a manual stop-start, or migrated within a cluster. The key concept behind Akka
Persistence is that only the _events_ that are persisted by the actor are stored, not the actual state of the actor
(although actor state snapshot support is available). The events are persisted by appending to storage (nothing is ever mutated) which
allows for very high transaction rates and efficient replication. A stateful actor is recovered by replaying the stored
events to the actor, allowing it to rebuild its state. This can be either the full history of changes
or starting from a checkpoint in a snapshot, which can dramatically reduce recovery times. 

Akka Persistence also supports @ref:[Durable State Behaviors](durable-state/persistence.md), which is based on 
persistence of the latest state of the actor. In this implementation, the _latest_ state is persisted, instead of events. 
Hence this is more similar to CRUD based applications.

The [Event Sourcing with Akka 2.6 video](https://akka.io/blog/news/2020/01/07/akka-event-sourcing-video)
is a good starting point for learning Event Sourcing, together with the @extref[Microservices with Akka tutorial](platform-guide:microservices-tutorial/) 
that illustrates how to implement an Event Sourced CQRS application with Akka Persistence and Akka Projections.

### Event Sourcing concepts

See an [introduction to Event Sourcing](https://docs.microsoft.com/en-us/previous-versions/msp-n-p/jj591559%28v=pandp.10%29) at MSDN.

Another excellent article about "thinking in Events" is [Events As First-Class Citizens](https://hackernoon.com/events-as-first-class-citizens-8633e8479493)
by Randy Shoup. It is a short and recommended read if you're starting developing Events based applications.
 
What follows is Akka's implementation via event sourced actors. 

An event sourced actor (also known as a persistent actor) receives a (non-persistent) command
which is first validated if it can be applied to the current state. Here validation can mean anything, from simple
inspection of a command message's fields up to a conversation with several external services, for example.
If validation succeeds, events are generated from the command, representing the effect of the command. These events
are then persisted and, after successful persistence, used to change the actor's state. When the event sourced actor
needs to be recovered, only the persisted events are replayed of which we know that they can be successfully applied.
In other words, events cannot fail when being replayed to a persistent actor, in contrast to commands. Event sourced
actors may also process commands that do not change application state such as query commands for example.

## Example and core API

Let's start with a simple example. The minimum required for a @apidoc[EventSourcedBehavior] is:

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #structure }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #structure }

The first important thing to notice is the @apidoc[typed.Behavior] of a persistent actor is typed to the type of the `Command`
because this is the type of message a persistent actor should receive. In Akka this is now enforced by the type system.

The components that make up an @apidoc[typed.*.EventSourcedBehavior] are:

* `persistenceId` is the stable unique identifier for the persistent actor.
* `emptyState` defines the `State` when the entity is first created e.g. a Counter would start with 0 as state.
* `commandHandler` defines how to handle command by producing Effects e.g. persisting events, stopping the persistent actor.
* `eventHandler` returns the new state given the current state when an event has been persisted.

@@@ div { .group-java }

Note that the concrete class does not contain any fields with state like a regular POJO. All state of the 
`EventSourcedBehavior` must be represented in the `State` or else they will not be persisted and therefore be
lost when the actor is stopped or restarted. Updates to the State are always performed in the eventHandler 
based on the events.

If the state is mutable, it is important that the `emptyState` method creates a new State instance each time
it is called to ensure that the state is recreated in case of failure restarts.

@@@

@@@ div { .group-scala }

It is recommended to use an immutable state class. If the state is mutable, it is important to use
the `EventSourcedBehavior.withMutableState` factory method that takes `emptyStateFactory: () => State` parameter
to make sure that the state instance is recreated in case of failure restarts.

@@@

Next we'll discuss each of these in detail.

### PersistenceId

The @apidoc[akka.persistence.typed.PersistenceId] is the stable unique identifier for the persistent actor in the backend
event journal and snapshot store.

@ref:[Cluster Sharding](cluster-sharding.md) is typically used together with `EventSourcedBehavior` to ensure
that there is only one active entity for each `PersistenceId` (`entityId`). There are techniques to ensure this 
uniqueness, an example of which can be found in the 
@ref:[Persistence example in the Cluster Sharding documentation](cluster-sharding.md#persistence-example). This illustrates how to construct the `PersistenceId` from the `entityTypeKey` and `entityId` provided by the @apidoc[typed.*.EntityContext].

The `entityId` in Cluster Sharding is the business domain identifier of the entity. The `entityId` might not
be unique enough to be used as the `PersistenceId` by itself. For example two different types of
entities may have the same `entityId`. To create a unique `PersistenceId` the `entityId` should be prefixed
with a stable name of the entity type, which typically is the same as the `EntityTypeKey.name` that
is used in Cluster Sharding. There are @scala[`PersistenceId.apply`]@java[`PersistenceId.of`] factory methods
to help with constructing such `PersistenceId` from an `entityTypeHint` and `entityId`.

The default separator when concatenating the `entityTypeHint` and `entityId` is `|`, but a custom separator
is supported.

@@@ note

The `|` separator is also used in Lagom's `scaladsl.PersistentEntity` but no separator is used
in Lagom's `javadsl.PersistentEntity`. For compatibility with Lagom's `javadsl.PersistentEntity`
you should use `""` as the separator.

@@@

A custom identifier can be created with @apidoc[PersistenceId.ofUniqueId](typed.PersistenceId$) {scala="#ofUniqueId(id:String):akka.persistence.typed.PersistenceId" java="#ofUniqueId(java.lang.String)"}.  

### Command handler

The command handler is a function with 2 parameters, the current `State` and the incoming `Command`.

A command handler returns an @scala[@scaladoc[Effect](akka.persistence.typed.scaladsl.Effect)]@java[@javadoc[Effect](akka.persistence.typed.javadsl.Effect)] directive that defines what event or events, if any, to persist. 
Effects are created using @java[a factory that is returned via the `Effect()` method] @scala[the `Effect` factory].

The two most commonly used effects are: 

* `persist` will persist one single event or several events atomically, i.e. all events
  are stored or none of them are stored if there is an error
* `none` no events are to be persisted, for example a read-only command

More effects are explained in @ref:[Effects and Side Effects](#effects-and-side-effects).

In addition to returning the primary `Effect` for the command `EventSourcedBehavior`s can also 
chain side effects that are to be performed after successful persist which is achieved with the `thenRun`
function e.g. @scala[`Effect.persist(..).thenRun`]@java[`Effect().persist(..).thenRun`].

### Event handler

When an event has been persisted successfully the new state is created by applying the event to the current state with the `eventHandler`.
In the case of multiple persisted events, the `eventHandler` is called with each event in the same order as they were passed to @scala[`Effect.persist(..)`]@java[`Effect().persist(..)`].

The state is typically defined as an immutable class and then the event handler returns a new instance of the state.
You may choose to use a mutable class for the state, and then the event handler may update the state instance and
return the same instance. Both immutable and mutable state is supported, but it must only be modified in the event handler.

The same event handler is also used when the entity is started up to recover its state from the stored events.

The event handler must only update the state and never perform side effects, as those would also be
executed during recovery of the persistent actor. Side effects should be performed in `thenRun` from the
@ref:[command handler](#command-handler) after persisting the event or from the @apidoc[typed.RecoveryCompleted]
after @ref:[Recovery](#recovery).

### Completing the example

Let's fill in the details of the example.

Command and event:

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #command }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #command }

State is a List containing the 5 latest items:

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #state }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #state }

The command handler persists the `Add` payload in an `Added` event:

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #command-handler }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #command-handler }

The event handler appends the item to the state and keeps 5 items. This is called after successfully
persisting the event in the database:

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #event-handler }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #event-handler }

@scala[These are used to create an @scaladoc[EventSourcedBehavior](akka.persistence.typed.scaladsl.EventSourcedBehavior):]
@java[These are defined in an @javadoc[EventSourcedBehavior](akka.persistence.typed.javadsl.EventSourcedBehavior):]

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #behavior }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #behavior }

## Effects and Side Effects

A command handler returns an @apidoc[typed.(scaladsl|javadsl).Effect] directive that defines what event or events, if any, to persist. 
Effects are created using @java[a factory that is returned via the `Effect()` method] @scala[the `Effect` factory]
and can be one of: 

* @scala[@scaladoc[persist](akka.persistence.typed.scaladsl.Effect$#persist[Event,State](events:Seq[Event]):akka.persistence.typed.scaladsl.EffectBuilder[Event,State])]@java[@javadoc[persist](akka.persistence.typed.javadsl.EffectFactories#persist(java.util.List))] will persist one single event or several events atomically, i.e. all events
  are stored or none of them are stored if there is an error
* @scala[@scaladoc[none](akka.persistence.typed.scaladsl.Effect$#none[Event,State]:akka.persistence.typed.scaladsl.EffectBuilder[Event,State])]@java[@javadoc[none](akka.persistence.typed.javadsl.EffectFactories#none())] no events are to be persisted, for example a read-only command
* @scala[@scaladoc[unhandled](akka.persistence.typed.scaladsl.Effect$#unhandled[Event,State]:akka.persistence.typed.scaladsl.EffectBuilder[Event,State])]@java[@javadoc[unhandled](akka.persistence.typed.javadsl.EffectFactories#unhandled())] the command is unhandled (not supported) in current state
* @scala[@scaladoc[stop](akka.persistence.typed.scaladsl.Effect$#stop[Event,State]():akka.persistence.typed.scaladsl.EffectBuilder[Event,State])]@java[@javadoc[stop](akka.persistence.typed.javadsl.EffectFactories#stop())] stop this actor
* @scala[@scaladoc[stash](akka.persistence.typed.scaladsl.Effect$#stash[Event,State]():akka.persistence.typed.scaladsl.ReplyEffect[Event,State])]@java[@javadoc[stash](akka.persistence.typed.javadsl.EffectFactories#stash())] the current command is stashed
* @scala[@scaladoc[unstashAll](akka.persistence.typed.scaladsl.Effect$#unstashAll[Event,State]():akka.persistence.typed.scaladsl.Effect[Event,State])]@java[@javadoc[unstashAll](akka.persistence.typed.javadsl.EffectFactories#unstashAll())] process the commands that were stashed with @scala[`Effect.stash`]@java[`Effect().stash`]
* @scala[@scaladoc[reply](akka.persistence.typed.scaladsl.Effect$#reply[ReplyMessage,Event,State](replyTo:akka.actor.typed.ActorRef[ReplyMessage])(replyWithMessage:ReplyMessage):akka.persistence.typed.scaladsl.ReplyEffect[Event,State])]@java[@javadoc[reply](akka.persistence.typed.javadsl.EffectFactories#reply(akka.actor.typed.ActorRef,ReplyMessage))] send a reply message to the given @apidoc[typed.ActorRef]
* @scala[@scaladoc[async](akka.persistence.typed.scaladsl.Effect$#async)]@java[@javadoc[async](akka.persistence.typed.javadsl.EffectFactories#async(java.util.concurrent.CompletionStage))] Asynchronous command handling
* @scala[@scaladoc[asyncReply](akka.persistence.typed.scaladsl.Effect$#asyncReply)]@java[@javadoc[asyncReply](akka.persistence.typed.javadsl.EffectFactories#asyncReply(java.util.concurrent.CompletionStage))] Asynchronous command handling and then reply

Note that only one of those can be chosen per incoming command. It is not possible to both persist and say none/unhandled.

In addition to returning the primary `Effect` for the command @apidoc[typed.*.EventSourcedBehavior]s can also 
chain side effects that are to be performed after successful persist which is achieved with the @apidoc[thenRun](typed.(scaladsl|javadsl).EffectBuilder) {scala="#thenRun(callback:State=%3EUnit):akka.persistence.typed.scaladsl.EffectBuilder[Event,State]" java="#thenRun(akka.japi.function.Effect)"}
function e.g. @scala[`Effect.persist(..).thenRun`]@java[`Effect().persist(..).thenRun`].

In the example below the state is sent to the `subscriber` ActorRef. Note that the new state after applying 
the event is passed as parameter of the `thenRun` function. In the case where multiple events have been persisted,
the state passed to `thenRun` is the updated state after all events have been handled.

All `thenRun` registered callbacks are executed sequentially after successful execution of the persist statement
(or immediately, in case of `none` and `unhandled`).

In addition to `thenRun` the following actions can also be performed after successful persist:

* @apidoc[thenStop](typed.(scaladsl|javadsl).EffectBuilder) {scala="#thenStop():akka.persistence.typed.scaladsl.EffectBuilder[Event,State]" java="#thenStop()"} the actor will be stopped
* @apidoc[thenUnstashAll](typed.(scaladsl|javadsl).EffectBuilder) {scala="#thenUnstashAll():akka.persistence.typed.scaladsl.Effect[Event,State]" java="#thenUnstashAll()"} process the commands that were stashed with @scala[`Effect.stash`]@java[`Effect().stash`]
* @apidoc[thenReply](typed.(scaladsl|javadsl).EffectBuilder) {scala="#thenReply[ReplyMessage](replyTo:akka.actor.typed.ActorRef[ReplyMessage])(replyWithMessage:State=%3EReplyMessage):akka.persistence.typed.scaladsl.ReplyEffect[Event,State]" java="#thenReply(akka.actor.typed.ActorRef,akka.japi.function.Function)"} send a reply message to the given @apidoc[typed.ActorRef]

Example of effects:

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #effects }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #effects }

Most of the time this will be done with the @apidoc[thenRun](typed.(scaladsl|javadsl).EffectBuilder) {scala="#thenRun(callback:State=%3EUnit):akka.persistence.typed.scaladsl.EffectBuilder[Event,State]" java="#thenRun(akka.japi.function.Effect)"} method on the `Effect` above. You can factor out
common side effects into functions and reuse for several commands. For example:

Scala
:  @@snip [PersistentActorCompileOnlyTest.scala](/akka-persistence-typed/src/test/scala/akka/persistence/typed/scaladsl/PersistentActorCompileOnlyTest.scala) { #commonChainedEffects }

Java
:  @@snip [PersistentActorCompileOnlyTest.java](/akka-persistence-typed/src/test/java/akka/persistence/typed/javadsl/PersistentActorCompileOnlyTest.java) { #commonChainedEffects }

### Side effects ordering and guarantees

Any side effects are executed on an at-most-once basis and will not be executed if the persist fails.

Side effects are not run when the actor is restarted or started again after being stopped.
You may inspect the state when receiving the @apidoc[typed.RecoveryCompleted] signal and execute side effects that
have not been acknowledged at that point. That may possibly result in executing side effects more than once.

The side effects are executed sequentially, it is not possible to execute side effects in parallel, unless they
call out to something that is running concurrently (for example sending a message to another actor).

It's possible to execute a side effects before persisting the event, but that can result in that the
side effect is performed but the event is not stored if the persist fails.

### Atomic writes

It is possible to store several events atomically by using the @scala[@scaladoc[persist](akka.persistence.typed.scaladsl.Effect$#persist[Event,State](events:Seq[Event]):akka.persistence.typed.scaladsl.EffectBuilder[Event,State])]@java[@javadoc[persist](akka.persistence.typed.javadsl.EffectFactories#persist(java.util.List))] effect with a list of events.
That means that all events passed to that method are stored or none of them are stored if there is an error.

The recovery of a persistent actor will therefore never be done partially with only a subset of events persisted by
a single @scala[@scaladoc[persist](akka.persistence.typed.scaladsl.Effect$#persist[Event,State](event:Event):akka.persistence.typed.scaladsl.EffectBuilder[Event,State])]@java[@javadoc[persist](akka.persistence.typed.javadsl.EffectFactories#persist(Event))] effect.

Some journals may not support atomic writes of several events and they will then reject the `persist` with
multiple events. This is signalled to an @apidoc[typed.*.EventSourcedBehavior] via an @apidoc[typed.PersistRejected] signal.
An @apidoc[typed.EventRejectedException] is also thrown (typically with a 
@javadoc[UnsupportedOperationException](java.lang.UnsupportedOperationException)) and can be handled with a @ref[supervisor](fault-tolerance.md).

## Cluster Sharding and EventSourcedBehavior

@ref:[Cluster Sharding](cluster-sharding.md) is an excellent fit to spread persistent actors over a
cluster, addressing them by id. It makes it possible to have more persistent actors exist in the cluster than what 
would fit in the memory of one node. Cluster sharding improves the resilience of the cluster. If a node crashes, 
the persistent actors are quickly started on a new node and can resume operations.

The @apidoc[typed.*.EventSourcedBehavior] can then be run as with any plain actor as described in @ref:[actors documentation](actors.md),
but since Akka Persistence is based on the single-writer principle the persistent actors are typically used together
with Cluster Sharding. For a particular `persistenceId` only one persistent actor instance should be active at one time.
If multiple instances were to persist events at the same time, the events would be interleaved and might not be
interpreted correctly on replay. Cluster Sharding ensures that there is only one active entity for each id. The
@ref:[Cluster Sharding example](cluster-sharding.md#persistence-example) illustrates this common combination.

## Accessing the ActorContext

If the @apidoc[EventSourcedBehavior] needs to use the @apidoc[typed.*.ActorContext], for example to spawn child actors, it can be obtained by
wrapping construction with @apidoc[Behaviors.setup](typed.*.Behaviors$) {scala="#setup[T](factory:akka.actor.typed.scaladsl.ActorContext[T]=%3Eakka.actor.typed.Behavior[T]):akka.actor.typed.Behavior[T]" java="#setup(akka.japi.function.Function)"}:

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #actor-context }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #actor-context }

## Changing Behavior

After processing a message, actors are able to return the @apidoc[typed.Behavior] that is used
for the next message.

As you can see in the above examples this is not supported by persistent actors. Instead, the state is
returned by `eventHandler`. The reason a new behavior can't be returned is that behavior is part of the actor's
state and must also carefully be reconstructed during recovery. If it would have been supported it would mean
that the behavior must be restored when replaying events and also encoded in the state anyway when snapshots are used.
That would be very prone to mistakes and thus not allowed in Akka Persistence.

For basic actors you can use the same set of command handlers independent of what state the entity is in,
as shown in above example. For more complex actors it's useful to be able to change the behavior in the sense
that different functions for processing commands may be defined depending on what state the actor is in.
This is useful when implementing finite state machine (FSM) like entities.

The next example demonstrates how to define different behavior based on the current `State`. It shows an actor that
represents the state of a blog post. Before a post is started the only command it can process is to `AddPost`.
Once it is started then one can look it up with `GetPost`, modify it with `ChangeBody` or publish it with `Publish`.

The state is captured by:

Scala
:  @@snip [BlogPostEntity.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BlogPostEntity.scala) { #state }

Java
:  @@snip [BlogPostEntity.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BlogPostEntity.java) { #state }

The commands, of which only a subset are valid depending on the state:

Scala
:  @@snip [BlogPostEntity.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BlogPostEntity.scala) { #commands }

Java
:  @@snip [BlogPostEntity.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BlogPostEntity.java) { #commands }

@java[The command handler to process each command is decided by the state class (or state predicate) that is
given to the `forStateType` of the @javadoc[CommandHandlerBuilder](akka.persistence.typed.javadsl.CommandHandlerBuilder) and the match cases in the builders.]
@scala[The command handler to process each command is decided by first looking at the state and then the command.
It typically becomes two levels of pattern matching, first on the state and then on the command.]
Delegating to methods is a good practice because the one-line cases give a nice overview of the message dispatch.

Scala
:  @@snip [BlogPostEntity.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BlogPostEntity.scala) { #command-handler }

Java
:  @@snip [BlogPostEntity.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BlogPostEntity.java) { #command-handler }


The event handler:

Scala
:  @@snip [BlogPostEntity.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BlogPostEntity.scala) { #event-handler }

Java
:  @@snip [BlogPostEntity.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BlogPostEntity.java) { #event-handler }

And finally the behavior is created @scala[from the @scaladoc[EventSourcedBehavior.apply](akka.persistence.typed.scaladsl.EventSourcedBehavior$#apply[Command,Event,State](persistenceId:akka.persistence.typed.PersistenceId,emptyState:State,commandHandler:(State,Command)=%3Eakka.persistence.typed.scaladsl.Effect[Event,State],eventHandler:(State,Event)=%3EState):akka.persistence.typed.scaladsl.EventSourcedBehavior[Command,Event,State])]:

Scala
:  @@snip [BlogPostEntity.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BlogPostEntity.scala) { #behavior }

Java
:  @@snip [BlogPostEntity.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BlogPostEntity.java) { #behavior }

This can be taken one or two steps further by defining the event and command handlers in the state class as
illustrated in @ref:[event handlers in the state](persistence-style.md#event-handlers-in-the-state) and
@ref:[command handlers in the state](persistence-style.md#command-handlers-in-the-state).

There is also an example illustrating an @ref:[optional initial state](persistence-style.md#optional-initial-state).

## Replies

The @ref:[Request-Response interaction pattern](interaction-patterns.md#request-response) is very common for
persistent actors, because you typically want to know if the command was rejected due to validation errors and
when accepted you want a confirmation when the events have been successfully stored.

Therefore you typically include a @apidoc[typed.ActorRef]@scala[`[ReplyMessageType]`]@java[`<ReplyMessageType>`]. If the 
command can either have a successful response or a validation error returned, the generic response type @apidoc[pattern.StatusReply]@scala[`[ReplyType]`]
@java[`<ReplyType>`] can be used. If the successful reply does not contain a value but is more of an acknowledgement
a pre defined @scala[@scaladoc[StatusReply.Ack](akka.pattern.StatusReply$#Ack:akka.pattern.StatusReply[akka.Done])]@java[@javadoc[StatusReply.ack()](akka.pattern.StatusReply$#ack():akka.pattern.StatusReply[akka.Done])] of type @scala[`StatusReply[Done]`]@java[`StatusReply<Done>`]
can be used.

After validation errors or after persisting events, using a @apidoc[thenRun](typed.(scaladsl|javadsl).EffectBuilder) {scala="#thenRun(callback:State=%3EUnit):akka.persistence.typed.scaladsl.EffectBuilder[Event,State]" java="#thenRun(akka.japi.function.Effect)"} side effect, the reply message can
be sent to the @apidoc[typed.ActorRef].

Scala
:  @@snip [BlogPostEntity.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BlogPostEntity.scala) { #reply-command }

Java
:  @@snip [BlogPostEntity.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BlogPostEntity.java) { #reply-command }


Scala
:  @@snip [BlogPostEntity.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BlogPostEntity.scala) { #reply }

Java
:  @@snip [BlogPostEntity.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BlogPostEntity.java) { #reply }


Since this is such a common pattern there is a reply effect for this purpose. It has the nice property that
it can be used to enforce that replies are not forgotten when implementing the @apidoc[typed.*.EventSourcedBehavior].
If it's defined with @scala[@scaladoc[EventSourcedBehavior.withEnforcedReplies](akka.persistence.typed.scaladsl.EventSourcedBehavior$#withEnforcedReplies[Command,Event,State](persistenceId:akka.persistence.typed.PersistenceId,emptyState:State,commandHandler:(State,Command)=%3Eakka.persistence.typed.scaladsl.ReplyEffect[Event,State],eventHandler:(State,Event)=%3EState):akka.persistence.typed.scaladsl.EventSourcedBehavior[Command,Event,State])]@java[@javadoc[EventSourcedBehaviorWithEnforcedReplies](akka.persistence.typed.javadsl.EventSourcedBehaviorWithEnforcedReplies)]
there will be compilation errors if the returned effect isn't a @apidoc[typed.(scaladsl|javadsl).ReplyEffect], which can be
created with @scala[`Effect.reply`]@java[`Effect().reply`], @scala[`Effect.noReply`]@java[`Effect().noReply`],
@scala[`Effect.thenReply`]@java[`Effect().thenReply`], or @scala[`Effect.thenNoReply`]@java[`Effect().thenNoReply`].

Scala
:  @@snip [AccountExampleWithEventHandlersInState.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/AccountExampleWithEventHandlersInState.scala) { #withEnforcedReplies }

Java
:  @@snip [AccountExampleWithNullState.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/AccountExampleWithEventHandlersInState.java) { #withEnforcedReplies }

The commands must have a field of @apidoc[typed.ActorRef]@scala[`[ReplyMessageType]`]@java[`<ReplyMessageType>`] that can then be used to send a reply.

Scala
:  @@snip [AccountExampleWithEventHandlersInState.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/AccountExampleWithEventHandlersInState.scala) { #reply-command }

Java
:  @@snip [AccountExampleWithNullState.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/AccountExampleWithEventHandlersInState.java) { #reply-command }

The @apidoc[typed.(scaladsl|javadsl).ReplyEffect] is created with @scala[`Effect.reply`]@java[`Effect().reply`], @scala[`Effect.noReply`]@java[`Effect().noReply`],
@scala[`Effect.thenReply`]@java[`Effect().thenReply`], or @scala[`Effect.thenNoReply`]@java[`Effect().thenNoReply`].

@java[Note that command handlers are defined with `newCommandHandlerWithReplyBuilder` when using
@javadoc[EventSourcedBehaviorWithEnforcedReplies](akka.persistence.typed.javadsl.EventSourcedBehaviorWithEnforcedReplies), as opposed to `newCommandHandlerBuilder` when using @javadoc[EventSourcedBehavior](akka.persistence.typed.javadsl.EventSourcedBehavior).]

Scala
:  @@snip [AccountExampleWithEventHandlersInState.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/AccountExampleWithEventHandlersInState.scala) { #reply }

Java
:  @@snip [AccountExampleWithNullState.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/AccountExampleWithEventHandlersInState.java) { #reply }

These effects will send the reply message even when @scala[@scaladoc[EventSourcedBehavior.withEnforcedReplies](akka.persistence.typed.scaladsl.EventSourcedBehavior$#withEnforcedReplies[Command,Event,State](persistenceId:akka.persistence.typed.PersistenceId,emptyState:State,commandHandler:(State,Command)=%3Eakka.persistence.typed.scaladsl.ReplyEffect[Event,State],eventHandler:(State,Event)=%3EState):akka.persistence.typed.scaladsl.EventSourcedBehavior[Command,Event,State])]@java[@javadoc[EventSourcedBehaviorWithEnforcedReplies](akka.persistence.typed.javadsl.EventSourcedBehaviorWithEnforcedReplies)]
is not used, but then there will be no compilation errors if the reply decision is left out.

Note that the `noReply` is a way of making conscious decision that a reply shouldn't be sent for a specific
command or the reply will be sent later, perhaps after some asynchronous interaction with other actors or services.

@@@ div { .group-scala }

It is recommended to use an immutable state class. If the state is mutable, it is important to use
the `EventSourcedBehavior.withEnforcedRepliesMutableState` factory method that takes `emptyStateFactory: () => State`
parameter to make sure that the state instance is recreated in case of failure restarts.

@@@

## Serialization

The same @ref:[serialization](../serialization.md) mechanism as for actor messages is also used for persistent actors.
When picking a serialization solution for the events you should also consider that it must be possible to read old events
when the application has evolved.
Strategies for that can be found in the @ref:[schema evolution](../persistence-schema-evolution.md).

You need to enable @ref:[serialization](../serialization.md) for your commands (messages), events, and state (snapshot).
@ref:[Serialization with Jackson](../serialization-jackson.md) is a good choice in many cases and our
recommendation if you don't have other preference.

## Recovery

An event sourced actor is automatically recovered on start and on restart by replaying journaled events.
New messages sent to the actor during recovery do not interfere with replayed events.
They are stashed and received by the @apidoc[typed.*.EventSourcedBehavior] after the recovery phase completes.

The number of concurrent recoveries that can be in progress at the same time is limited
to not overload the system and the backend data store. When exceeding the limit the actors will wait
until other recoveries have been completed. This is configured by:

```
akka.persistence.max-concurrent-recoveries = 50
```

The @ref:[event handler](#event-handler) is used for updating the state when replaying the journaled events.

It is strongly discouraged to perform side effects in the event handler, so side effects should be performed
once recovery has completed as a reaction to the @apidoc[typed.RecoveryCompleted] signal @scala[in the @scaladoc[receiveSignal](akka.persistence.typed.scaladsl.EventSourcedBehavior#receiveSignal(signalHandler:PartialFunction[(State,akka.actor.typed.Signal),Unit]):akka.persistence.typed.scaladsl.EventSourcedBehavior[Command,Event,State]) handler] @java[by overriding @javadoc[receiveSignal](akka.persistence.typed.javadsl.SignalHandlerBuilder#onSignal(java.lang.Class,java.util.function.BiConsumer))]

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #recovery }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #recovery }

The `RecoveryCompleted` contains the current `State`.

The actor will always receive a `RecoveryCompleted` signal, even if there are no events
in the journal and the snapshot store is empty, or if it's a new persistent actor with a previously
unused `PersistenceId`.

@ref[Snapshots](persistence-snapshot.md) can be used for optimizing recovery times.

### Replay filter

There could be cases where event streams are corrupted and multiple writers (i.e. multiple persistent actor instances)
journaled different messages with the same sequence number.
In such a case, you can configure how you filter replayed messages from multiple writers, upon recovery.

In your configuration, under the `akka.persistence.journal.xxx.replay-filter` section (where `xxx` is your journal plugin id),
you can select the replay filter `mode` from one of the following values:

 * repair-by-discard-old
 * fail
 * warn
 * off

For example, if you configure the replay filter for leveldb plugin, it looks like this:

```
# The replay filter can detect a corrupt event stream by inspecting
# sequence numbers and writerUuid when replaying events.
akka.persistence.journal.leveldb.replay-filter {
  # What the filter should do when detecting invalid events.
  # Supported values:
  # `repair-by-discard-old` : discard events from old writers,
  #                           warning is logged
  # `fail` : fail the replay, error is logged
  # `warn` : log warning but emit events untouched
  # `off` : disable this feature completely
  mode = repair-by-discard-old
}
```

### Disable recovery

You can also completely disable the recovery of events and snapshots:

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #recovery-disabled }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #recovery-disabled }

Please refer to @ref[snapshots](persistence-snapshot.md#snapshots) if you need to disable only the snapshot recovery, or you need to select specific snapshots.

In any case, the highest sequence number will always be recovered so you can keep persisting new events without corrupting your event log.

@@@ warning

Disable of recovery is not normal behavior of an event sourced actor, since events and snapshots are not used for
the recovery of the actor.

@@@

### Recovery from only last event

For some use cases it is enough to recover the actor from the last event, as an optimization to not replay all events.
You can enable this recovery mode with:

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #replay-last }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #replay-last }

Snapshots are not loaded when recovery from last event is selected.

@@@ warning

Recovery from only last event is not normal behavior of an event sourced actor, since it typically would need
all events, or a snapshot and events after the snapshot, to recover its state.

This feature is currently only supported by the R2DBC plugin.

@@@

## Tagging

Persistence allows you to use event tags without using an @ref[`EventAdapter`](../persistence.md#event-adapters):

Scala
:  @@snip [BasicPersistentActorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #tagging }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #tagging }

## Event adapters

Event adapters can be programmatically added to your @apidoc[typed.*.EventSourcedBehavior]s that can convert from your `Event` type
to another type that is then passed to the journal.

Defining an event adapter is done by extending an EventAdapter:

Scala
:  @@snip [x](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #event-wrapper }

Java
:  @@snip [x](/akka-persistence-typed/src/test/java/akka/persistence/typed/javadsl/PersistentActorCompileOnlyTest.java) { #event-wrapper }

Then install it on an `EventSourcedBehavior`:

Scala
:  @@snip [x](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #install-event-adapter }

Java
:  @@snip [x](/akka-persistence-typed/src/test/java/akka/persistence/typed/javadsl/PersistentActorCompileOnlyTest.java) { #install-event-adapter }

## Wrapping EventSourcedBehavior

When creating an @apidoc[typed.*.EventSourcedBehavior], it is possible to wrap `EventSourcedBehavior` in
other behaviors such as @apidoc[Behaviors.setup](typed.*.Behaviors$) {scala="#setup[T](factory:akka.actor.typed.scaladsl.ActorContext[T]=%3Eakka.actor.typed.Behavior[T]):akka.actor.typed.Behavior[T]" java="#setup(akka.japi.function.Function)"} in order to access the @apidoc[typed.*.ActorContext] object. For instance
to access the actor logging upon taking snapshots for debug purpose.

Scala
:  @@snip [BasicPersistentActorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #wrapPersistentBehavior }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #wrapPersistentBehavior }


## Journal failures

By default an @apidoc[typed.*.EventSourcedBehavior] will stop if an exception is thrown from the journal. It is possible to override this with
any @apidoc[typed.BackoffSupervisorStrategy]. It is not possible to use the normal supervision wrapping for this as it isn't valid to
`resume` a behavior on a journal failure as it is not known if the event was persisted.

Scala
:  @@snip [BasicPersistentBehaviorSpec.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #supervision }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #supervision }

If there is a problem with recovering the state of the actor from the journal, a @apidoc[typed.RecoveryFailed] signal is
emitted to the @scala[@scaladoc[receiveSignal](akka.persistence.typed.scaladsl.EventSourcedBehavior#receiveSignal(signalHandler:PartialFunction[(State,akka.actor.typed.Signal),Unit]):akka.persistence.typed.scaladsl.EventSourcedBehavior[Command,Event,State]) handler] @java[@javadoc[receiveSignal](akka.persistence.typed.javadsl.SignalHandlerBuilder#onSignal(java.lang.Class,java.util.function.BiConsumer)) method] and the actor will be stopped
(or restarted with backoff).

If there is a problem with persisting an event to the journal, a @apidoc[typed.PersistFailed] signal is
emitted to the @scala[@scaladoc[receiveSignal](akka.persistence.typed.scaladsl.EventSourcedBehavior#receiveSignal(signalHandler:PartialFunction[(State,akka.actor.typed.Signal),Unit]):akka.persistence.typed.scaladsl.EventSourcedBehavior[Command,Event,State]) handler] @java[@javadoc[receiveSignal](akka.persistence.typed.javadsl.SignalHandlerBuilder#onSignal(java.lang.Class,java.util.function.BiConsumer)) method] and the actor will be stopped
(or restarted with backoff).

### Journal rejections

Journals can reject events. The difference from a failure is that the journal must decide to reject an event before
trying to persist it e.g. because of a serialization exception. If an event is rejected it definitely won't be in the journal.
This is signalled to an @apidoc[typed.*.EventSourcedBehavior] via an @apidoc[typed.PersistRejected] signal.
An @apidoc[typed.EventRejectedException] is also thrown and can be handled with a @ref[supervisor](fault-tolerance.md).
Not all journal implementations use rejections and treat these kind of problems also as journal failures. 

## Stash

When persisting events with @scala[@scaladoc[persist](akka.persistence.typed.scaladsl.Effect$#persist[Event,State](events:Seq[Event]):akka.persistence.typed.scaladsl.EffectBuilder[Event,State])]@java[@javadoc[persist](akka.persistence.typed.javadsl.EffectFactories#persist(java.util.List))] it is guaranteed that the @apidoc[typed.*.EventSourcedBehavior] will not receive
further commands until after the events have been confirmed to be persisted and additional side effects have been run.
Incoming messages are stashed automatically until the `persist` is completed.

Commands are also stashed during recovery and will not interfere with replayed events. Commands will be received
when recovery has been completed.

The stashing described above is handled automatically, but there is also a possibility to stash commands when
they are received to defer processing of them until later. One example could be waiting for some external condition
or interaction to complete before processing additional commands. That is accomplished by returning a @scala[@scaladoc[stash](akka.persistence.typed.scaladsl.Effect$#stash[Event,State]():akka.persistence.typed.scaladsl.ReplyEffect[Event,State])]@java[@javadoc[stash](akka.persistence.typed.javadsl.EffectFactories#stash())] effect
and later use @apidoc[thenUnstashAll](typed.(scaladsl|javadsl).EffectBuilder) {scala="#thenUnstashAll():akka.persistence.typed.scaladsl.Effect[Event,State]" java="#thenUnstashAll()"}.

Let's use an example of a task manager to illustrate how the stashing effects can be used. It handles three commands;
`StartTask`, `NextStep` and `EndTask`. Those commands are associated with a given `taskId` and the manager processes
one `taskId` at a time. A task is started when receiving `StartTask`, and continues when receiving `NextStep` commands
until the final `EndTask` is received. Commands with another `taskId` than the one in progress are deferred by
stashing them. When `EndTask` is processed a new task can start and the stashed commands are processed.

Scala
:  @@snip [StashingExample.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/StashingExample.scala) { #stashing }

Java
:  @@snip [StashingExample.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/StashingExample.java) { #stashing }

You should be careful to not send more messages to a persistent actor than it can keep up with, otherwise the stash
buffer will fill up and when reaching its maximum capacity the commands will be dropped. The capacity can be configured with:

```
akka.persistence.typed.stash-capacity = 10000
```

To override the global config from above, use the following api to define a custom stash buffer capacity per entity:

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #custom-stash-buffer }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #custom-stash-buffer }

Note that the stashed commands are kept in an in-memory buffer, so in case of a crash they will not be
processed.

* Stashed commands are discarded in case the actor (entity) is passivated or rebalanced by Cluster Sharding.
* Stashed commands are discarded in case the actor is restarted (or stopped) due to a thrown exception while processing a command or side effect after persisting.
* Stashed commands are preserved and processed later in case of a failure while storing events but only if an `onPersistFailure` backoff supervisor strategy is defined.

It's allowed to stash messages while unstashing. Those newly added commands will not be processed by the
@scala[@scaladoc[unstashAll](akka.persistence.typed.scaladsl.Effect$#unstashAll[Event,State]():akka.persistence.typed.scaladsl.Effect[Event,State])]@java[@javadoc[unstashAll](akka.persistence.typed.javadsl.EffectFactories#unstashAll())] effect that was in progress and have to be unstashed by another `unstashAll`.

## Scaling out

In a use case where the number of persistent actors needed is higher than what would fit in the memory of one node or
where resilience is important so that if a node crashes the persistent actors are quickly started on a new node and can
resume operations @ref:[Cluster Sharding](cluster-sharding.md) is an excellent fit to spread persistent actors over a 
cluster and address them by id.

Akka Persistence is based on the single-writer principle. For a particular @apidoc[typed.PersistenceId] only one @apidoc[typed.*.EventSourcedBehavior]
instance should be active at one time. If multiple instances were to persist events at the same time, the events would
be interleaved and might not be interpreted correctly on replay. Cluster Sharding ensures that there is only one
active entity (`EventSourcedBehavior`) for each id within the cluster.
@ref:[Replicated Event Sourcing](replicated-eventsourcing.md) supports active-active persistent entities.

## Configuration

There are several configuration properties for the persistence module, please refer
to the @ref:[reference configuration](../general/configuration-reference.md#config-akka-persistence).

The @ref:[journal and snapshot store plugins](../persistence-plugins.md) have specific configuration, see
reference documentation of the chosen plugin.

## Example project

The @extref[Microservices with Akka tutorial](platform-guide:microservices-tutorial/) contains a
Shopping Cart sample illustrating how to use Event Sourcing and Projections together. The events are
consumed by even processors to build other representations from the events, or publish the events
to other services.

The @extref[Akka Distributed Cluster Guide](akka-distributed-cluster:guide/3-active-active.html) illustrates how to use @ref:[Replicated Event Sourcing](replicated-eventsourcing.md) that supports
active-active persistent entities.

