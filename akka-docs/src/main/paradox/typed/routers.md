# Routers

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Routing](../routing.md).

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Actor Typed, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-actor-typed_$scala.binary.version$
  version=AkkaVersion
}

## Introduction

In some cases it is useful to distribute messages of the same type over a set of actors, so that messages can be
processed in parallel - a single actor will only process one message at a time.

The router itself is a behavior that is spawned into a running actor that will then forward any message sent to it
to one final recipient out of the set of routees.

There are two kinds of routers included in Akka Typed - the pool router and the group router.

## Pool Router

The pool router is created with a routee `Behavior` and spawns a number of children with that behavior which it will
then forward messages to.

If a child is stopped the pool router removes it from its set of routees. When the last child stops the router itself stops.
To make a resilient router that deals with failures the routee `Behavior` must be supervised.

As actor children are always local the routees are never spread across a cluster with a pool router.

Let's first introduce the routee:

Scala
:  @@snip [RouterSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/RouterSpec.scala) { #routee }

Java
:  @@snip [RouterTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/RouterTest.java) { #routee }

After having defined the routee, we can now concentrate on configuring the router itself. Note again the the router is an Actor in itself:

Scala
:  @@snip [RouterSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/RouterSpec.scala) { #pool }

Java
:  @@snip [RouterTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/RouterTest.java) { #pool }

### Configuring Dispatchers

Since the router itself is spawned as an actor the dispatcher used for it can be configured directly in the call to `spawn`.
The routees, however, are spawned by the router.
Therefore, the `PoolRouter` has a property to configure the `Props` of its routees:

Scala
:  @@snip [RouterSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/RouterSpec.scala) { #pool-dispatcher }

Java
:  @@snip [RouterTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/RouterTest.java) { #pool-dispatcher }

### Broadcasting a message to all routees

Pool routers can be configured to identify messages intended to be broad-casted to all routees.
Therefore, the `PoolRouter` has a property to configure its `broadcastPredicate`:

Scala
:  @@snip [RouterSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/RouterSpec.scala) { #broadcast }

Java
:  @@snip [RouterTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/RouterTest.java) { #broadcast }

## Group Router

The group router is created with a `ServiceKey` and uses the receptionist (see @ref:[Receptionist](actor-discovery.md#receptionist)) to discover
available actors for that key and routes messages to one of the currently known registered actors for a key.

Since the receptionist is used this means the group router is cluster-aware out of the box. The router sends
messages to registered actors on any node in the cluster that is reachable. If no reachable actor exists the router
will fallback and route messages to actors on nodes marked as unreachable.

That the receptionist is used also means that the set of routees is eventually consistent, and that immediately when
the group router is started the set of routees it knows about is empty, until it has seen a listing from the receptionist
it stashes incoming messages and forwards them as soon as it gets a listing from the receptionist.  

When the router has received a listing from the receptionist and the set of registered actors is empty the router will
drop them (publish them to the event stream as `akka.actor.Dropped`).

Scala
:  @@snip [RouterSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/RouterSpec.scala) { #group }

Java
:  @@snip [RouterTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/RouterTest.java) { #group }

## Routing strategies

There are three different strategies for selecting which routee a message is forwarded to that can be selected
from the router before spawning it:

Scala
:  @@snip [RouterSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/RouterSpec.scala) { #strategy }

Java
:  @@snip [RouterTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/RouterTest.java) { #strategy }

### Round Robin

Rotates over the set of routees making sure that if there are `n` routees, then for `n` messages
sent through the router, each actor is forwarded one message.

Round robin gives fair routing where every available routee gets the same amount of messages as long as the set
of routees stays relatively stable, but may be unfair if the set of routees changes a lot.

This is the default for pool routers as the pool of routees is expected to remain the same.

An optional parameter `preferLocalRoutees` can be used for this strategy. Routers will only use routees located in local actor system if `preferLocalRoutees` is true and local routees do exist. The default value for this parameter is false.

### Random

Randomly selects a routee when a message is sent through the router.

This is the default for group routers as the group of routees is expected to change as nodes join and leave the cluster.

An optional parameter `preferLocalRoutees` can be used for this strategy. Routers will only use routees located in local actor system if `preferLocalRoutees` is true and local routees do exist. The default value for this parameter is false.

### Consistent Hashing

Uses [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing) to select a routee based
on the sent message. This [article](http://www.tom-e-white.com/2007/11/consistent-hashing.html)
gives good insight into how consistent hashing is implemented.

Currently you have to define hashMapping of the router to map incoming messages to their consistent
hash key. This makes the decision transparent for the sender.

Consistent hashing delivers messages with the same hash to the same routee as long as the set of routees stays the same.
When the set of routees changes, consistent hashing tries to make sure, but does not guarantee, that messages with the same hash are routed to the same routee.


Scala
:  @@snip [RouterSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/RouterSpec.scala) { #consistent-hashing }

Java
:  @@snip [RouterTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/RouterTest.java) { #consistent-hashing }

See also @ref[Akka Cluster Sharding](cluster-sharding.md) which provides stable routing and rebalancing of the routee actors.

## Routers and performance

Note that if the routees are sharing a resource, the resource will determine if increasing the number of
actors will actually give higher throughput or faster answers. For example if the routees are CPU bound actors
it will not give better performance to create more routees than there are threads to execute the actors.

Since the router itself is an actor and has a mailbox this means that messages are routed sequentially to the routees
where it can be processed in parallel (depending on the available threads in the dispatcher).
In a high throughput use cases the sequential routing could become a bottle neck. Akka Typed does not provide an optimized tool for this.
