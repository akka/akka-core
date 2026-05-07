# Classic Routing

@@include[includes.md](includes.md) { #actor-api }
For the documentation of the new API of this feature and for new projects see @ref:[routers](typed/routers.md).

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Routing, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor_$scala.binary.version$"
  version=AkkaVersion
}

## Introduction

Messages can be sent via a router to efficiently route them to destination actors, known as
its *routees*. A @apidoc[akka.routing.Router] can be used inside or outside of an actor, and you can manage the
routees yourselves or use a self contained router actor with configuration capabilities.

Different routing strategies can be used, according to your application's needs. Akka comes with
several useful routing strategies right out of the box. But, as you will see in this chapter, it is
also possible to @ref:[create your own](#custom-router).

<a id="simple-router"></a>
## A Simple Router

The following example illustrates how to use a `Router` and manage the routees from within an actor.

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #router-in-actor }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #router-in-actor }

We create a `Router` and specify that it should use @apidoc[akka.routing.RoundRobinRoutingLogic] when routing the
messages to the routees.

The routing logic shipped with Akka are:

 * @apidoc[akka.routing.RoundRobinRoutingLogic]
 * @apidoc[akka.routing.RandomRoutingLogic]
 * @apidoc[akka.routing.SmallestMailboxRoutingLogic]
 * @apidoc[akka.routing.BroadcastRoutingLogic]
 * @apidoc[akka.routing.ScatterGatherFirstCompletedRoutingLogic]
 * @apidoc[akka.routing.TailChoppingRoutingLogic]
 * @apidoc[akka.routing.ConsistentHashingRoutingLogic]

We create the routees as ordinary child actors wrapped in @apidoc[akka.routing.ActorRefRoutee]. We watch
the routees to be able to replace them if they are terminated.

Sending messages via the router is done with the @apidoc[route](akka.routing.Router) {scala="#route(message:Any,sender:akka.actor.ActorRef):Unit" java="#route(java.lang.Object,akka.actor.ActorRef)"} method, as is done for the `Work` messages
in the example above.

The `Router` is immutable and the @apidoc[akka.routing.RoutingLogic] is thread safe; meaning that they can also be used
outside of actors.  

@@@ note

In general, any message sent to a router will be sent onwards to its routees, but there is one exception.
The special @ref:[Broadcast Messages](#broadcast-messages) will send to *all* of a router's routees.
However, do not use @ref:[Broadcast Messages](#broadcast-messages) when you use @ref:[BalancingPool](#balancing-pool) for routees
as described in @ref:[Specially Handled Messages](#router-special-messages).

@@@

## A Router Actor

A router can also be created as a self contained actor that manages the routees itself and
loads routing logic and other settings from configuration.

This type of router actor comes in two distinct flavors:

 * Pool - The router creates routees as child actors and removes them from the router if they
terminate.
 * Group - The routee actors are created externally to the router and the router sends
messages to the specified path using actor selection, without watching for termination.

The settings for a router actor can be defined in configuration or programmatically. 
In order to make an actor to make use of an externally configurable router the `FromConfig` props wrapper must be used
to denote that the actor accepts routing settings from configuration.
This is in contrast with Remote Deployment where such marker props is not necessary.
If the props of an actor is NOT wrapped in `FromConfig` it will ignore the router section of the deployment configuration.

You send messages to the routees via the router actor in the same way as for ordinary actors,
i.e. via its @apidoc[akka.actor.ActorRef]. The router actor forwards messages onto its routees without changing 
the original sender. When a routee replies to a routed message, the reply will be sent to the 
original sender, not to the router actor.

@@@ note

In general, any message sent to a router will be sent onwards to its routees, but there are a
few exceptions. These are documented in the @ref:[Specially Handled Messages](#router-special-messages) section below.

@@@

### Pool

The following code and configuration snippets show how to create a @ref:[round-robin](#round-robin-router) router that forwards messages to five `Worker` routees. The
routees will be created as the router's children.

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-round-robin-pool }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #round-robin-pool-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #round-robin-pool-1 }

Here is the same example, but with the router configuration provided programmatically instead of
from configuration.

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #round-robin-pool-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #round-robin-pool-2 }

#### Remote Deployed Routees

In addition to being able to create local actors as routees, you can instruct the router to
deploy its created children on a set of remote hosts. Routees will be deployed in round-robin
fashion. In order to deploy routees remotely, wrap the router configuration in a
@apidoc[akka.remote.routing.RemoteRouterConfig], attaching the remote addresses of the nodes to deploy to. Remote
deployment requires the `akka-remote` module to be included in the classpath.

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #remoteRoutees }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #remoteRoutees }

#### Senders

By default, when a routee sends a message, it will @ref:[implicitly set itself as the sender
](actors.md#actors-tell-sender).

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #reply-without-sender }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #reply-with-self }

However, it is often useful for routees to set the *router* as a sender. For example, you might want
to set the router as the sender if you want to hide the details of the routees behind the router.
The following code snippet shows how to set the parent router as sender.

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #reply-with-sender }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #reply-with-parent }

#### Supervision

Routees that are created by a pool router will be created as the router's children. The router is 
therefore also the children's supervisor.

The supervision strategy of the router actor can be configured with the
`supervisorStrategy` property of the Pool. If no configuration is provided, routers default
to a strategy of “always escalate”. This means that errors are passed up to the router's supervisor
for handling. The router's supervisor will decide what to do about any errors.

Note the router's supervisor will treat the error as an error with the router itself. Therefore a
directive to stop or restart will cause the router *itself* to stop or restart. The router, in
turn, will cause its children to stop and restart.

It should be mentioned that the router's restart behavior has been overridden so that a restart,
while still re-creating the children, will still preserve the same number of actors in the pool.

This means that if you have not specified `supervisorStrategy` of the router or its parent a
failure in a routee will escalate to the parent of the router, which will by default restart the router,
which will restart all routees (it uses Escalate and does not stop routees during restart). The reason 
is to make the default behave such that adding @apidoc[withRouter](akka.actor.Props) {scala="#withRouter(r:akka.routing.RouterConfig):akka.actor.Props" java="#withRouter(akka.routing.RouterConfig)"} to a child’s definition does not 
change the supervision strategy applied to the child. This might be an inefficiency that you can avoid 
by specifying the strategy when defining the router.

Setting the strategy is done like this:

Scala
:  @@snip [RoutingSpec.scala](/akka-actor-tests/src/test/scala/akka/routing/RoutingSpec.scala) { #supervision }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #supervision }

@@@ note

If the child of a pool router terminates, the pool router will not automatically spawn
a new child. In the event that all children of a pool router have terminated the
router will terminate itself unless it is a dynamic router, e.g. using
a resizer.

@@@

### Group

Sometimes, rather than having the router actor create its routees, it is desirable to create routees
separately and provide them to the router for its use. You can do this by passing in
paths of the routees to the router's configuration. Messages will be sent with @apidoc[akka.actor.ActorSelection] 
to these paths, wildcards can be and will result in the same semantics as explicitly using `ActorSelection`.

The example below shows how to create a router by providing it with the path strings of three
routee actors. 

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-round-robin-group }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #round-robin-group-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #round-robin-group-1 }

Here is the same example, but with the router configuration provided programmatically instead of
from configuration.

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #paths #round-robin-group-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #paths #round-robin-group-2 }

The routee actors are created externally from the router:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #create-workers }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #create-workers }


Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #create-worker-actors }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #create-worker-actors }

The paths may contain protocol and address information for actors running on remote hosts.
Remoting requires the `akka-remote` module to be included in the classpath.

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-remote-round-robin-group }

## Router usage

In this section we will describe how to create the different types of router actors.

The router actors in this section are created from within a top level actor named `parent`. 
Note that deployment paths in the configuration starts with `/parent/` followed by the name
of the router actor. 

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #create-parent }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #create-parent }

<a id="round-robin-router"></a>
### RoundRobinPool and RoundRobinGroup

Routes in a [round-robin](https://en.wikipedia.org/wiki/Round-robin) fashion to its routees.

RoundRobinPool defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-round-robin-pool }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #round-robin-pool-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #round-robin-pool-1 }

RoundRobinPool defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #round-robin-pool-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #round-robin-pool-2 }

RoundRobinGroup defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-round-robin-group }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #round-robin-group-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #round-robin-group-1 }

RoundRobinGroup defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #paths #round-robin-group-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #paths #round-robin-group-2 }

### RandomPool and RandomGroup

This router type selects one of its routees randomly for each message.

RandomPool defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-random-pool }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #random-pool-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #random-pool-1 }

RandomPool defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #random-pool-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #random-pool-2 }

RandomGroup defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-random-group }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #random-group-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #random-group-1 }

RandomGroup defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #paths #random-group-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #paths #random-group-2 }

<a id="balancing-pool"></a>
### BalancingPool

A Router that will try to redistribute work from busy routees to idle routees.
All routees share the same mailbox.

@@@ note

The BalancingPool has the property that its routees do not have truly distinct
identity: they have different names, but talking to them will not end up at the
right actor in most cases. Therefore you cannot use it for workflows that
require state to be kept within the routee, you would in this case have to
include the whole state in the messages.

With a @ref:[SmallestMailboxPool](#smallestmailboxpool) you can have a vertically scaling service that
can interact in a stateful fashion with other services in the back-end before
replying to the original client. The other advantage is that it does not place
a restriction on the message queue implementation as BalancingPool does.

@@@

@@@ note

Do not use @ref:[Broadcast Messages](#broadcast-messages) when you use @ref:[BalancingPool](#balancing-pool) for routers,
as described in @ref:[Specially Handled Messages](#router-special-messages).

@@@

BalancingPool defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-balancing-pool }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #balancing-pool-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #balancing-pool-1 }

BalancingPool defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #balancing-pool-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #balancing-pool-2 }

Addition configuration for the balancing dispatcher, which is used by the pool,
can be configured in the `pool-dispatcher` section of the router deployment
configuration.

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-balancing-pool2 }

The @apidoc[BalancingPool] automatically uses a special `BalancingDispatcher` for its
routees - disregarding any dispatcher that is set on the routee Props object.
This is needed in order to implement the balancing semantics via
sharing the same mailbox by all the routees.

While it is not possible to change the dispatcher used by the routees, it is possible
to fine tune the used *executor*. By default the `fork-join-dispatcher` is used and
can be configured as explained in @ref:[Dispatchers](dispatchers.md). In situations where the
routees are expected to perform blocking operations it may be useful to replace it
with a `thread-pool-executor` hinting the number of allocated threads explicitly:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-balancing-pool3 }

It is also possible to change the `mailbox` used by the balancing dispatcher for
scenarios where the default unbounded mailbox is not well suited. An example of such
a scenario could arise whether there exists the need to manage priority for each message.
You can then implement a priority mailbox and configure your dispatcher:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-balancing-pool4 }

@@@ note

Bear in mind that `BalancingDispatcher` requires a message queue that must be thread-safe for
multiple concurrent consumers. So it is mandatory for the message queue backing a custom mailbox
for this kind of dispatcher to implement akka.dispatch.MultipleConsumerSemantics. See details
on how to implement your custom mailbox in @ref:[Mailboxes](mailboxes.md).

@@@

There is no Group variant of the BalancingPool.

### SmallestMailboxPool

A Router that tries to send to the non-suspended child routee with fewest messages in mailbox.
The selection is done in this order:

 * pick any idle routee (not processing message) with empty mailbox
 * pick any routee with empty mailbox
 * pick routee with fewest pending messages in mailbox
 * pick any remote routee, remote actors are consider lowest priority,
since their mailbox size is unknown

SmallestMailboxPool defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-smallest-mailbox-pool }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #smallest-mailbox-pool-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #smallest-mailbox-pool-1 }

SmallestMailboxPool defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #smallest-mailbox-pool-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #smallest-mailbox-pool-2 }

There is no Group variant of the SmallestMailboxPool because the size of the mailbox
and the internal dispatching state of the actor is not practically available from the paths
of the routees.

### BroadcastPool and BroadcastGroup

A broadcast router forwards the message it receives to *all* its routees.

BroadcastPool defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-broadcast-pool }


Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #broadcast-pool-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #broadcast-pool-1 }

BroadcastPool defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #broadcast-pool-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #broadcast-pool-2 }

BroadcastGroup defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-broadcast-group }


Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #broadcast-group-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #broadcast-group-1 }

BroadcastGroup defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #paths #broadcast-group-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #paths #broadcast-group-2 }

@@@ note

Broadcast routers always broadcast *every* message to their routees. If you do not want to
broadcast every message, then you can use a non-broadcasting router and use
[Broadcast Messages](#broadcast-messages) as needed.

@@@

### ScatterGatherFirstCompletedPool and ScatterGatherFirstCompletedGroup

The ScatterGatherFirstCompletedRouter will send the message on to all its routees.
It then waits for first reply it gets back. This result will be sent back to original sender.
Other replies are discarded.

It is expecting at least one reply within a configured duration, otherwise it will reply with
@apidoc[akka.pattern.AskTimeoutException] in a @apidoc[akka.actor.Status.Failure].

ScatterGatherFirstCompletedPool defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-scatter-gather-pool }


Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #scatter-gather-pool-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #scatter-gather-pool-1 }

ScatterGatherFirstCompletedPool defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #scatter-gather-pool-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #scatter-gather-pool-2 }

ScatterGatherFirstCompletedGroup defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-scatter-gather-group }


Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #scatter-gather-group-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #scatter-gather-group-1 }

ScatterGatherFirstCompletedGroup defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #paths #scatter-gather-group-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #paths #scatter-gather-group-2 }

### TailChoppingPool and TailChoppingGroup

The TailChoppingRouter will first send the message to one, randomly picked, routee
and then after a small delay to a second routee (picked randomly from the remaining routees) and so on.
It waits for first reply it gets back and forwards it back to original sender. Other replies are discarded.

The goal of this router is to decrease latency by performing redundant queries to multiple routees, assuming that
one of the other actors may still be faster to respond than the initial one.

This optimisation was described nicely in a blog post by Peter Bailis:
[Doing redundant work to speed up distributed queries](http://www.bailis.org/blog/doing-redundant-work-to-speed-up-distributed-queries/).

TailChoppingPool defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-tail-chopping-pool }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #tail-chopping-pool-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #tail-chopping-pool-1 }

TailChoppingPool defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #tail-chopping-pool-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #tail-chopping-pool-2 }

TailChoppingGroup defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-tail-chopping-group }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #tail-chopping-group-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #tail-chopping-group-1 }

TailChoppingGroup defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #paths #tail-chopping-group-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #paths #tail-chopping-group-2 }

### ConsistentHashingPool and ConsistentHashingGroup

The ConsistentHashingPool uses [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing)
to select a routee based on the sent message. This 
[article](https://tom-e-white.com/2007/11/consistent-hashing.html) gives good 
insight into how consistent hashing is implemented.

There is 3 ways to define what data to use for the consistent hash key.

 * You can define @scala[@scaladoc[hashMapping](akka.routing.ConsistentHashingPool#hashMapping:akka.routing.ConsistentHashingRouter.ConsistentHashMapping)]@java[@javadoc[withHashMapper](akka.routing.ConsistentHashingRoutingLogic#withHashMapper(akka.routing.ConsistentHashingRouter.ConsistentHashMapper))] of the router to map incoming
messages to their consistent hash key. This makes the decision
transparent for the sender.
 * The messages may implement @apidoc[akka.routing.ConsistentHashingRouter.ConsistentHashable].
The key is part of the message and it's convenient to define it together
with the message definition.
 * The messages can be wrapped in a @apidoc[akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope]
to define what data to use for the consistent hash key. The sender knows
the key to use.

These ways to define the consistent hash key can be use together and at
the same time for one router. The @scala[`hashMapping`]@java[`withHashMapper`] is tried first.

Code example:

Scala
:  @@snip [ConsistentHashingRouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/ConsistentHashingRouterDocSpec.scala) { #cache-actor }

Java
:  @@snip [ConsistentHashingRouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/ConsistentHashingRouterDocTest.java) { #cache-actor }


Scala
:  @@snip [ConsistentHashingRouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/ConsistentHashingRouterDocSpec.scala) { #consistent-hashing-router }

Java
:  @@snip [ConsistentHashingRouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/ConsistentHashingRouterDocTest.java) { #consistent-hashing-router }

In the above example you see that the `Get` message implements `ConsistentHashable` itself,
while the `Entry` message is wrapped in a `ConsistentHashableEnvelope`. The `Evict`
message is handled by the `hashMapping` partial function.

ConsistentHashingPool defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-consistent-hashing-pool }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #consistent-hashing-pool-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #consistent-hashing-pool-1 }

ConsistentHashingPool defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #consistent-hashing-pool-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #consistent-hashing-pool-2 }

ConsistentHashingGroup defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-consistent-hashing-group }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #consistent-hashing-group-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #consistent-hashing-group-1 }

ConsistentHashingGroup defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #paths #consistent-hashing-group-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #paths #consistent-hashing-group-2 }

`virtual-nodes-factor` is the number of virtual nodes per routee that is used in the 
consistent hash node ring to make the distribution more uniform.

<a id="router-special-messages"></a>
## Specially Handled Messages

Most messages sent to router actors will be forwarded according to the routers' routing logic.
However there are a few types of messages that have special behavior.

Note that these special messages, except for the @apidoc[akka.routing.Broadcast] message, are only handled by 
self contained router actors and not by the @apidoc[akka.routing.Router] component described 
in @ref:[A Simple Router](#simple-router).

### Broadcast Messages

A `Broadcast` message can be used to send a message to *all* of a router's routees. When a router
receives a `Broadcast` message, it will broadcast that message's *payload* to all routees, no
matter how that router would normally route its messages.

The example below shows how you would use a `Broadcast` message to send a very important message
to every routee of a router.

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #broadcastDavyJonesWarning }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #broadcastDavyJonesWarning }

In this example the router receives the `Broadcast` message, extracts its payload
(`"Watch out for Davy Jones' locker"`), and then sends the payload on to all of the router's
routees. It is up to each routee actor to handle the received payload message.

@@@ note

Do not use @ref:[Broadcast Messages](#broadcast-messages) when you use @ref:[BalancingPool](#balancing-pool) for routers.
Routees on @ref:[BalancingPool](#balancing-pool) shares the same mailbox instance, thus some routees can
possibly get the broadcast message multiple times, while other routees get no broadcast message.

@@@

### PoisonPill Messages

A @apidoc[akka.actor.PoisonPill] message has special handling for all actors, including for routers. When any actor
receives a `PoisonPill` message, that actor will be stopped. See the @ref:[PoisonPill](actors.md#poison-pill)
documentation for details.

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #poisonPill }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #poisonPill }

For a router, which normally passes on messages to routees, it is important to realise that
`PoisonPill` messages are processed by the router only. `PoisonPill` messages sent to a router
will *not* be sent on to routees.

However, a `PoisonPill` message sent to a router may still affect its routees, because it will
stop the router and when the router stops it also stops its children. Stopping children is normal
actor behavior. The router will stop routees that it has created as children. Each child will
process its current message and then stop. This may lead to some messages being unprocessed.
See the documentation on @ref:[Stopping actors](actors.md#stopping-actors) for more information.

If you wish to stop a router and its routees, but you would like the routees to first process all
the messages currently in their mailboxes, then you should not send a `PoisonPill` message to the
router. Instead you should wrap a `PoisonPill` message inside a `Broadcast` message so that each
routee will receive the `PoisonPill` message. Note that this will stop all routees, even if the
routees aren't children of the router, i.e. even routees programmatically provided to the router.

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #broadcastPoisonPill }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #broadcastPoisonPill }

With the code shown above, each routee will receive a `PoisonPill` message. Each routee will
continue to process its messages as normal, eventually processing the `PoisonPill`. This will
cause the routee to stop. After all routees have stopped the router will itself be stopped
automatically unless it is a dynamic router, e.g. using a resizer.

### Kill Messages

@apidoc[akka.actor.Kill] messages are another type of message that has special handling. See
@ref:[Killing an Actor](actors.md#killing-actors) for general information about how actors handle `Kill` messages.

When a `Kill` message is sent to a router the router processes the message internally, and does
*not* send it on to its routees. The router will throw an @apidoc[ActorKilledException] and fail. It
will then be either resumed, restarted or terminated, depending how it is supervised.

Routees that are children of the router will also be suspended, and will be affected by the
supervision directive that is applied to the router. Routees that are not the routers children, i.e.
those that were created externally to the router, will not be affected.

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #kill }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #kill }

As with the @apidoc[akka.actor.PoisonPill] message, there is a distinction between killing a router, which
indirectly kills its children (who happen to be routees), and killing routees directly (some of whom
may not be children.) To kill routees directly the router should be sent a `Kill` message wrapped
in a @apidoc[akka.routing.Broadcast] message.

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #broadcastKill }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #broadcastKill }

### Management Messages

 * Sending @apidoc[akka.routing.GetRoutees] to a router actor will make it send back its currently used routees
in a @apidoc[akka.routing.Routees] message.
 * Sending @apidoc[akka.routing.AddRoutee] to a router actor will add that routee to its collection of routees.
 * Sending @apidoc[akka.routing.RemoveRoutee] to a router actor will remove that routee to its collection of routees.
 * Sending @apidoc[akka.routing.AdjustPoolSize] to a pool router actor will add or remove that number of routees to
its collection of routees.

These management messages may be handled after other messages, so if you send `AddRoutee` immediately followed by
an ordinary message you are not guaranteed that the routees have been changed when the ordinary message
is routed. If you need to know when the change has been applied you can send `AddRoutee` followed by `GetRoutees`
and when you receive the `Routees` reply you know that the preceding change has been applied.

<a id="resizable-routers"></a>
## Dynamically Resizable Pool

@scala[Most]@java[All] pools can be used with a fixed number of routees or with a resize strategy to adjust the number
of routees dynamically.

There are two types of resizers: the default @apidoc[akka.routing.Resizer] and the @apidoc[akka.routing.OptimalSizeExploringResizer].

### Default Resizer

The default resizer ramps up and down pool size based on pressure, measured by the percentage of busy routees
in the pool. It ramps up pool size if the pressure is higher than a certain threshold and backs off if the
pressure is lower than certain threshold. Both thresholds are configurable.

Pool with default resizer defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-resize-pool }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #resize-pool-1 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #resize-pool-1 }

Several more configuration options are available and described in `akka.actor.deployment.default.resizer`
section of the reference @ref:[configuration](general/configuration.md).

Pool with resizer defined in code:

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #resize-pool-2 }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #resize-pool-2 }

*It is also worth pointing out that if you define the ``router`` in the configuration file then this value
will be used instead of any programmatically sent parameters.*

### Optimal Size Exploring Resizer

The @apidoc[akka.routing.OptimalSizeExploringResizer] resizes the pool to an optimal size that provides the most message throughput.

This resizer works best when you expect the pool size to performance function to be a convex function.
For example, when you have a CPU bound tasks, the optimal size is bound to the number of CPU cores.
When your task is IO bound, the optimal size is bound to optimal number of concurrent connections to that IO service -
e.g. a 4 node elastic search cluster may handle 4-8 concurrent requests at optimal speed.

It achieves this by keeping track of message throughput at each pool size and performing the following
three resizing operations (one at a time) periodically:

 * Downsize if it hasn't seen all routees ever fully utilized for a period of time.
 * Explore to a random nearby pool size to try and collect throughput metrics.
 * Optimize to a nearby pool size with a better (than any other nearby sizes) throughput metrics.

When the pool is fully-utilized (i.e. all routees are busy), it randomly choose between exploring and optimizing.
When the pool has not been fully-utilized for a period of time, it will downsize the pool to the last seen max
utilization multiplied by a configurable ratio.

By constantly exploring and optimizing, the resizer will eventually walk to the optimal size and
remain nearby. When the optimal size changes it will start walking towards the new one.

It keeps a performance log so it's stateful as well as having a larger memory footprint than the default `Resizer`.
The memory usage is O(n) where n is the number of sizes you allow, i.e. upperBound - lowerBound.

Pool with `OptimalSizeExploringResizer` defined in configuration:

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-optimal-size-exploring-resize-pool }

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #optimal-size-exploring-resize-pool }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #optimal-size-exploring-resize-pool }

Several more configuration options are available and described in `akka.actor.deployment.default.optimal-size-exploring-resizer`
section of the reference @ref:[configuration](general/configuration.md).

@@@ note

Resizing is triggered by sending messages to the actor pool, but it is not
completed synchronously; instead a message is sent to the “head”
`RouterActor` to perform the size change. Thus you cannot rely on resizing
to instantaneously create new workers when all others are busy, because the
message just sent will be queued to the mailbox of a busy actor. To remedy
this, configure the pool to use a balancing dispatcher, see [Configuring
Dispatchers](#configuring-dispatchers) for more information.

@@@

<a id="router-design"></a>
## How Routing is Designed within Akka

On the surface routers look like normal actors, but they are actually implemented differently.
Routers are designed to be extremely efficient at receiving messages and passing them quickly on to
routees.

A normal actor can be used for routing messages, but an actor's single-threaded processing can
become a bottleneck. Routers can achieve much higher throughput with an optimization to the usual
message-processing pipeline that allows concurrent routing. This is achieved by embedding routers'
routing logic directly in their @apidoc[akka.actor.ActorRef] rather than in the router actor. Messages sent to
a router's `ActorRef` can be immediately routed to the routee, bypassing the single-threaded
router actor entirely.

The cost to this is that the internals of routing code are more complicated than if
routers were implemented with normal actors. Fortunately all of this complexity is invisible to
consumers of the routing API. However, it is something to be aware of when implementing your own
routers.

## Custom Router

You can create your own router should you not find any of the ones provided by Akka sufficient for your needs.
In order to roll your own router you have to fulfill certain criteria which are explained in this section.

Before creating your own router you should consider whether a normal actor with router-like
behavior might do the job just as well as a full-blown router. As explained
[above](#router-design), the primary benefit of routers over normal actors is their
higher performance. But they are somewhat more complicated to write than normal actors. Therefore if
lower maximum throughput is acceptable in your application you may wish to stick with traditional
actors. This section, however, assumes that you wish to get maximum performance and so demonstrates
how you can create your own router.

The router created in this example is replicating each message to a few destinations.

Start with the routing logic:

Scala
:  @@snip [CustomRouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/CustomRouterDocSpec.scala) { #routing-logic }

Java
:  @@snip [CustomRouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/CustomRouterDocTest.java) { #routing-logic }

`select` will be called for each message and in this example pick a few destinations by round-robin,
by reusing the existing @apidoc[akka.routing.RoundRobinRoutingLogic] and wrap the result in a @apidoc[akka.routing.SeveralRoutees]
instance.  `SeveralRoutees` will send the message to all of the supplied routes.

The implementation of the routing logic must be thread safe, since it might be used outside of actors.

A unit test of the routing logic: 

Scala
:  @@snip [CustomRouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/CustomRouterDocSpec.scala) { #unit-test-logic }

Java
:  @@snip [CustomRouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/CustomRouterDocTest.java) { #unit-test-logic }

You could stop here and use the `RedundancyRoutingLogic` with a `akka.routing.Router`
as described in @ref:[A Simple Router](#simple-router).

Let us continue and make this into a self contained, configurable, router actor.

Create a class that extends @apidoc[akka.routing.Pool], @apidoc[akka.routing.Group] or @apidoc[akka.routing.CustomRouterConfig]. That class is a factory
for the routing logic and holds the configuration for the router. Here we make it a `Group`.

Scala
:  @@snip [CustomRouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/CustomRouterDocSpec.scala) { #group }

Java
:  @@snip [RedundancyGroup.java](/akka-docs/src/test/java/jdocs/routing/RedundancyGroup.java) { #group }

This can be used exactly as the router actors provided by Akka.

Scala
:  @@snip [CustomRouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/CustomRouterDocSpec.scala) { #usage-1 }

Java
:  @@snip [CustomRouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/CustomRouterDocTest.java) { #usage-1 }

Note that we added a constructor in `RedundancyGroup` that takes a @javadoc[Config](com.typesafe.config.Config) parameter.
That makes it possible to define it in configuration.

Scala
:  @@snip [CustomRouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/CustomRouterDocSpec.scala) { #config }

Java
:  @@snip [CustomRouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/CustomRouterDocSpec.scala) { #jconfig }

Note the fully qualified class name in the `router` property. The router class must extend
`akka.routing.RouterConfig` (`Pool`, `Group` or `CustomRouterConfig`) and have 
constructor with one `com.typesafe.config.Config` parameter.
The deployment section of the configuration is passed to the constructor.

Scala
:  @@snip [CustomRouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/CustomRouterDocSpec.scala) { #usage-2 }

Java
:  @@snip [CustomRouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/CustomRouterDocTest.java) { #usage-2 }

## Configuring Dispatchers

The dispatcher for created children of the pool will be taken from
@apidoc[akka.actor.Props] as described in @ref:[Dispatchers](dispatchers.md).

To make it easy to define the dispatcher of the routees of the pool you can
define the dispatcher inline in the deployment section of the config.

@@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #config-pool-dispatcher }

That is the only thing you need to do enable a dedicated dispatcher for a
pool.

@@@ note

If you use a group of actors and route to their paths, then they will still use the same dispatcher
that was configured for them in their `Props`, it is not possible to change an actors dispatcher
after it has been created.

@@@

The “head” router cannot always run on the same dispatcher, because it
does not process the same type of messages, hence this special actor does
not use the dispatcher configured in @apidoc[akka.actor.Props], but takes the
`routerDispatcher` from the @apidoc[akka.routing.RouterConfig] instead, which defaults to
the actor system’s default dispatcher. All standard routers allow setting this
property in their constructor or factory method, custom routers have to
implement the method in a suitable way.

Scala
:  @@snip [RouterDocSpec.scala](/akka-docs/src/test/scala/docs/routing/RouterDocSpec.scala) { #dispatchers }

Java
:  @@snip [RouterDocTest.java](/akka-docs/src/test/java/jdocs/routing/RouterDocTest.java) { #dispatchers }

@@@ note

It is not allowed to configure the `routerDispatcher` to be a
@apidoc[akka.dispatch.BalancingDispatcherConfigurator] since the messages meant
for the special router actor cannot be processed by any other actor.

@@@
