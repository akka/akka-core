# Actor discovery

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Actors](../actors.md#actorselection).

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

## Obtaining Actor references

There are two general ways to obtain @ref:[Actor references](../general/addressing.md#what-is-an-actor-reference): by
@ref:[creating actors](actor-lifecycle.md#creating-actors) and by discovery using the @ref:[Receptionist](#receptionist).

You can pass actor references between actors as constructor parameters or part of messages.

Sometimes you need something to bootstrap the interaction, for example when actors are running on
different nodes in the Cluster or when "dependency injection" with constructor parameters is not
applicable.

## Receptionist

When an actor needs to be discovered by another actor but you are unable to put a reference to it in an incoming message,
you can use the @apidoc[receptionist.Receptionist]. It supports both local and cluster (see @ref:[cluster](#cluster-receptionist)). You register the specific actors that should be discoverable 
from each node in the local `Receptionist` instance. The API of the receptionist is also based on actor messages. 
This registry of actor references is then automatically distributed to all other nodes in the case of a cluster. 
You can lookup such actors with the key that was used when they were registered. The reply to such a `Find` request is 
a `Listing`, which contains a `Set` of actor references that are registered for the key. Note that several actors can be 
registered to the same key.

The registry is dynamic. New actors can be registered during the lifecycle of the system. Entries are removed when 
registered actors are stopped, manually deregistered or the node they live on is removed from the @ref:[Cluster](cluster.md). 
To facilitate this dynamic aspect you can also subscribe to changes with the `Receptionist.Subscribe` message. It will send 
`Listing` messages to the subscriber, first with the set of entries upon subscription, then whenever the entries for a key are changed.

These imports are used in the following example:

Scala
:  @@snip [ReceptionistExample](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/ReceptionistExample.scala) { #import }

Java
:  @@snip [ReceptionistExample](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/ReceptionistExample.java) { #import }

First we create a `PingService` actor and register it with the `Receptionist` against a
@apidoc[receptionist.ServiceKey] that will later be used to lookup the reference:

Scala
:  @@snip [ReceptionistExample](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/ReceptionistExample.scala) { #ping-service }

Java
:  @@snip [ReceptionistExample](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/ReceptionistExample.java) { #ping-service }

Then we have another actor that requires a `PingService` to be constructed:

Scala
:  @@snip [ReceptionistExample](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/ReceptionistExample.scala) { #pinger }

Java
:  @@snip [ReceptionistExample](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/ReceptionistExample.java) { #pinger }

Finally in the guardian actor we spawn the service as well as subscribing to any actors registering
against the @apidoc[receptionist.ServiceKey]. Subscribing means that the guardian actor will be informed of any
new registrations via a `Listing` message:

Scala
:  @@snip [ReceptionistExample](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/ReceptionistExample.scala) { #pinger-guardian }

Java
:  @@snip [ReceptionistExample](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/ReceptionistExample.java) { #pinger-guardian }

Each time a new (which is just a single time in this example) `PingService` is registered the
guardian actor spawns a `Pinger` for each currently known `PingService`. The `Pinger`
sends a `Ping` message and when receiving the `Pong` reply it stops.

In above example we used `Receptionist.Subscribe`, but it's also possible to request a single `Listing`
of the current state without receiving further updates by sending the `Receptionist.Find` message to the
receptionist. An example of using `Receptionist.Find`:

Scala
:  @@snip [ReceptionistExample](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/ReceptionistExample.scala) { #find }

Java
:  @@snip [ReceptionistExample](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/ReceptionistExample.java) { #find }

Also note how a @apidoc[messageAdapter](akka.actor.typed.*.ActorContext) {scala="#messageAdapter[U](f:U=%3ET)(implicitevidence$1:scala.reflect.ClassTag[U]):akka.actor.typed.ActorRef[U]" java="#messageAdapter(java.lang.Class,akka.japi.function.Function)"} is used to convert the `Receptionist.Listing` to a message type that
the `PingManager` understands.

If a server no longer wish to be associated with a service key it can deregister using the command `Receptionist.Deregister`
which will remove the association and inform all subscribers.

The command can optionally send an acknowledgement once the local receptionist has removed the registration. The acknowledgement does not guarantee
that all subscribers has seen that the instance has been removed, it may still receive messages from subscribers for some time after this.

Scala
:  @@snip [ReceptionistExample](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/ReceptionistExample.scala) { #deregister }

Java
:  @@snip [ReceptionistExample](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/ReceptionistExample.java) { #deregister }

## Cluster Receptionist

The `Receptionist` also works in a cluster, an actor registered to the receptionist will appear in the receptionist 
of the other nodes of the cluster.

The state for the receptionist is propagated via @ref:[distributed data](distributed-data.md) which means that each node
will eventually reach the same set of actors per `ServiceKey`.

`Subscription`s and `Find` queries to a clustered receptionist will keep track of cluster reachability and only list 
registered actors that are reachable. The full set of actors, including unreachable ones, is available through 
@scala[@scaladoc[Listing.allServiceInstances](akka.actor.typed.receptionist.Receptionist.Listing#allServiceInstances[T](key:akka.actor.typed.receptionist.ServiceKey[T]):Set[akka.actor.typed.ActorRef[T]])]@java[@javadoc[Listing.getAllServiceInstances](akka.actor.typed.receptionist.Receptionist.Listing#getAllServiceInstances(akka.actor.typed.receptionist.ServiceKey))].

One important difference from local only receptions are the serialization concerns, all messages sent to and back from 
an actor on another node must be serializable, see @ref:[serialization](../serialization.md).

## Receptionist Scalability

The receptionist does not scale up to any number of services or very high turnaround of services. 
It will likely handle up to thousands or tens of thousands of services. Use cases with higher 
demands the receptionist for initial contact between actors on the nodes while the actual logic of those is up to the applications own actors. 
