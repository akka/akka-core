# Part 5: Querying Device Groups

## Introduction

The conversational patterns that we have seen so far are simple in the sense that they require the actor to keep little or no state. Specifically:

* Device actors return a reading, which requires no state change
* Record a temperature, which updates a single field
* Device Group actors maintain group membership by adding or removing entries from a map

In this part, we will use a more complex example. Since homeowners will be interested in the temperatures throughout their home, our goal is to be able to query all of the device actors in a group. Let us start by investigating how such a query API should behave.

## Dealing with possible scenarios
The very first issue we face is that the membership of a group is dynamic. Each sensor device is represented by an actor that can stop at any time. At the beginning of the query, we can ask all of the existing device actors for the current temperature. However, during the lifecycle of the query:

 * A device actor might stop and not be able to respond back with a temperature reading.
 * A new device actor might start up and not be included in the query because we weren't aware of it.

These issues can be addressed in many different ways, but the important point is to settle on the desired behavior. The following works well for our use case:

 * When a query arrives, the group actor takes a _snapshot_ of the existing device actors and will only ask those actors for the temperature.
 * Actors that start up _after_ the query arrives are ignored.
 * If an actor in the snapshot stops during the query without answering, we will report the fact that it stopped to the sender of the query message.

Apart from device actors coming and going dynamically, some actors might take a long time to answer. For example, they could be stuck in an accidental infinite loop, or fail due to a bug and drop our request. We don't want the query to continue indefinitely, so we will consider it complete in either of the following cases:

* All actors in the snapshot have either responded or have confirmed being stopped.
* We reach a pre-defined deadline.

Given these decisions, along with the fact that a device in the snapshot might have just started and not yet received a temperature to record, we can define four states
for each device actor, with respect to a temperature query:

 * It has a temperature available: `Temperature`.
 * It has responded, but has no temperature available yet: `TemperatureNotAvailable`.
 * It has stopped before answering: `DeviceNotAvailable`.
 * It did not respond before the deadline: `DeviceTimedOut`.

Summarizing these in message types we can add the following to the message protocol:

Scala
:   @@snip [DeviceGroup.scala](/akka-docs/src/test/scala/typed/tutorial_5/DeviceManager.scala) { #query-protocol }

Java
:   @@snip [DeviceGroup.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/DeviceManager.java) { #query-protocol }

## Implementing the query

One approach for implementing the query involves adding code to the device group actor. However, in practice this can be very cumbersome and error-prone. Remember that when we start a query, we need to take a snapshot of the devices present and start a timer so that we can enforce the deadline. In the meantime, _another query_ can arrive. For the second query we need to keep track of the exact same information but in isolation from the previous query. This would require us to maintain separate mappings between queries and device actors.

Instead, we will implement a simpler, and superior approach. We will create an actor that represents a _single query_ and that performs the tasks needed to complete the query on behalf of the group actor. So far we have created actors that belonged to classical domain objects, but now, we will create an
actor that represents a process or a task rather than an entity. We benefit by keeping our group device actor simple and being able to better test query capability in isolation.

### Defining the query actor

First, we need to design the lifecycle of our query actor. This consists of identifying its initial state, the first action it will take, and the cleanup &#8212; if necessary. The query actor will need the following information:

 * The snapshot and IDs of active device actors to query.
 * The ID of the request that started the query (so that we can include it in the reply).
 * The reference of the actor who sent the query. We will send the reply to this actor directly.
 * A deadline that indicates how long the query should wait for replies. Making this a parameter will simplify testing.

#### Scheduling the query timeout
Since we need a way to indicate how long we are willing to wait for responses, it is time to introduce a new Akka feature that we have
not used yet, the built-in scheduler facility. Using @apidoc[Behaviors.withTimers](typed.*.Behaviors$) {scala="#withTimers[T](factory:akka.actor.typed.scaladsl.TimerScheduler[T]=%3eakka.actor.typed.Behavior[T]):akka.actor.typed.Behavior[T]" java="#withTimers(akka.japi.function.Function)"} and @apidoc[startSingleTimer](typed.*.TimerScheduler) {scala="#startSingleTimer(key:Any,msg:T,delay:scala.concurrent.duration.FiniteDuration):Unit" java="#startSingleTimer(java.lang.Object,T,java.time.Duration)"} to schedule a message that will be sent after a given delay.


We need to create a message that represents the query timeout. We create a simple message `CollectionTimeout` without any parameters for this purpose.

At the start of the query, we need to ask each of the device actors for the current temperature. To be able to quickly
detect devices that stopped before they got the `ReadTemperature` message we will also watch each of the actors. This
way, we get `DeviceTerminated` messages for those that stop during the lifetime of the query, so we don't need to wait
until the timeout to mark these as not available.

Putting this together, the outline of our `DeviceGroupQuery` actor looks like this:

Scala
:   @@snip [DeviceGroupQuery.scala](/akka-docs/src/test/scala/typed/tutorial_5/DeviceGroupQuery.scala) { #query-outline }

Java
:   @@snip [DeviceGroupQuery.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/DeviceGroupQuery.java) { #query-outline }

Note that we have to convert the `RespondTemperature` replies from the device actor to the message protocol that the `DeviceGroupQuery` actor understands, i.e. `DeviceGroupQuery.Command`. For this we use a `messageAdapter` that wraps the `RespondTemperature` in a `WrappedRespondTemperature`, which @scala[extends]@java[implements] `DeviceGroupQuery.Command`.

#### Tracking actor state

The query actor, apart from the pending timer, has one stateful aspect, tracking the set of actors that: have replied, have stopped, or have not replied. We track this state @scala[in a `var` field of an immutable `Map`]@java[in a mutable `HashMap`] in the actor.

For our use case:

1. We keep track of the state with:
    * a `Map` of already received replies
    * a `Set` of actors that we still wait on
2. We have three events to act on:
    * We can receive a `RespondTemperature` message from one of the devices.
    * We can receive a `DeviceTerminated` message for a device actor that has been stopped in the meantime.
    * We can reach the deadline and receive a `CollectionTimeout`.

To accomplish this, add the following to your `DeviceGroupQuery` source file:

Scala
:   @@snip [DeviceGroupQuery.scala](/akka-docs/src/test/scala/typed/tutorial_5/DeviceGroupQuery.scala) { #query-state }

Java
:   @@snip [DeviceGroupQuery.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/DeviceGroupQuery.java) { #query-state }

For `RespondTemperature` and `DeviceTerminated` we keep track of the replies by updating `repliesSoFar` and remove the actor from `stillWaiting`. For this, we can use the actor's identifier already present in the `DeviceTerminated` message. For our `RespondTemperature` message we will need to add this information as follows:

Scala
:   @@snip [DeviceGroupQuery.scala](/akka-docs/src/test/scala/typed/tutorial_5/Device.scala) { #respond-declare }

Java
:   @@snip [DeviceGroupQuery.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/Device.java) { #respond-declare }

And:

Scala
:   @@snip [DeviceGroupQuery.scala](/akka-docs/src/test/scala/typed/tutorial_5/Device.scala) { #respond-reply }

Java
:   @@snip [DeviceGroupQuery.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/Device.java) { #respond-reply }


After processing each message we delegate to a method `respondWhenAllCollected`, which we will discuss soon.

In the case of timeout, we need to take all the actors that have not yet replied (the members of the set `stillWaiting`) and put a `DeviceTimedOut` as the status in the final reply.


We now have to figure out what to do in `respondWhenAllCollected`. First, we need to record the new result in the map `repliesSoFar` and remove the actor from `stillWaiting`. The next step is to check if there are any remaining actors we are waiting for. If there is none, we send the result of the query to the original requester and stop the query actor. Otherwise, we need to update the `repliesSoFar` and `stillWaiting` structures and wait for more messages.

With all this knowledge, we can create the `respondWhenAllCollected` method:

Scala
:   @@snip [DeviceGroupQuery.scala](/akka-docs/src/test/scala/typed/tutorial_5/DeviceGroupQuery.scala) { #query-collect-reply }

Java
:   @@snip [DeviceGroupQuery.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/DeviceGroupQuery.java) { #query-collect-reply }

Our query actor is now done:

Scala
:   @@snip [DeviceGroupQuery.scala](/akka-docs/src/test/scala/typed/tutorial_5/DeviceGroupQuery.scala) { #query-full }

Java
:   @@snip [DeviceGroupQuery.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/DeviceGroupQuery.java) { #query-full }

### Testing the query actor

Now let's verify the correctness of the query actor implementation. There are various scenarios we need to test individually to make
sure everything works as expected. To be able to do this, we need to simulate the device actors somehow to exercise
various normal or failure scenarios. Thankfully we took the list of collaborators (actually a `Map`) as a parameter
to the query actor, so we can pass in @apidoc[typed.*.TestProbe] references. In our first test, we try out the case when
there are two devices and both report a temperature:

Scala
:   @@snip [DeviceGroupQuerySpec.scala](/akka-docs/src/test/scala/typed/tutorial_5/DeviceGroupQuerySpec.scala) { #query-test-normal }

Java
:   @@snip [DeviceGroupQueryTest.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/DeviceGroupQueryTest.java) { #query-test-normal }

That was the happy case, but we know that sometimes devices cannot provide a temperature measurement. This
scenario is just slightly different from the previous:

Scala
:   @@snip [DeviceGroupQuerySpec.scala](/akka-docs/src/test/scala/typed/tutorial_5/DeviceGroupQuerySpec.scala) { #query-test-no-reading }

Java
:   @@snip [DeviceGroupQueryTest.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/DeviceGroupQueryTest.java) { #query-test-no-reading }

We also know, that sometimes device actors stop before answering:

Scala
:   @@snip [DeviceGroupQuerySpec.scala](/akka-docs/src/test/scala/typed/tutorial_5/DeviceGroupQuerySpec.scala) { #query-test-stopped }

Java
:   @@snip [DeviceGroupQueryTest.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/DeviceGroupQueryTest.java) { #query-test-stopped }

If you remember, there is another case related to device actors stopping. It is possible that we get a normal reply
from a device actor, but then receive a @apidoc[typed.Terminated] for the same actor later. In this case, we would like to keep
the first reply and not mark the device as `DeviceNotAvailable`. We should test this, too:

Scala
:   @@snip [DeviceGroupQuerySpec.scala](/akka-docs/src/test/scala/typed/tutorial_5/DeviceGroupQuerySpec.scala) { #query-test-stopped-later }

Java
:   @@snip [DeviceGroupQueryTest.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/DeviceGroupQueryTest.java) { #query-test-stopped-later }

The final case is when not all devices respond in time. To keep our test relatively fast, we will construct the
`DeviceGroupQuery` actor with a smaller timeout:

Scala
:   @@snip [DeviceGroupQuerySpec.scala](/akka-docs/src/test/scala/typed/tutorial_5/DeviceGroupQuerySpec.scala) { #query-test-timeout }

Java
:   @@snip [DeviceGroupQueryTest.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/DeviceGroupQueryTest.java) { #query-test-timeout }

Our query works as expected now, it is time to include this new functionality in the `DeviceGroup` actor now.

## Adding query capability to the group

Including the query feature in the group actor is fairly simple now. We did all the heavy lifting in the query actor
itself, the group actor only needs to create it with the right initial parameters and nothing else.

Scala
:   @@snip [DeviceGroup.scala](/akka-docs/src/test/scala/typed/tutorial_5/DeviceGroup.scala) { #query-added }

Java
:   @@snip [DeviceGroup.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/DeviceGroup.java) { #query-added }

It is probably worth restating what we said at the beginning of the chapter. By keeping the temporary state that is only relevant to the query itself in a separate actor we keep the group actor implementation very simple. It delegates
everything to child actors and therefore does not have to keep state that is not relevant to its core business. Also, multiple queries can now run parallel to each other, in fact, as many as needed. In our case querying an individual device actor is a fast operation, but if this were not the case, for example, because the remote sensors need to be contacted over the network, this design would significantly improve throughput.

We close this chapter by testing that everything works together. This test is a variant of the previous ones, now exercising the group query feature:

Scala
:   @@snip [DeviceGroupSpec.scala](/akka-docs/src/test/scala/typed/tutorial_5/DeviceGroupSpec.scala) { #group-query-integration-test }

Java
:   @@snip [DeviceGroupTest.java](/akka-docs/src/test/java/jdocs/typed/tutorial_5/DeviceGroupTest.java) { #group-query-integration-test }

## Summary
In the context of the IoT system, this guide introduced the following concepts, among others. You can follow the links to review them if necessary:

* @ref:[The hierarchy of actors and their lifecycle](tutorial_1.md)
* @ref:[The importance of designing messages for flexibility](tutorial_3.md)
* @ref:[How to watch and stop actors, if necessary](tutorial_4.md#keeping-track-of-the-device-actors-in-the-group)

## What's Next?

To continue your journey with Akka, we recommend:

* Start building your own applications with Akka, make sure you [get involved in our amazing community](https://akka.io/get-involved/) for help if you get stuck.
* If you’d like some additional background, and detail, read the rest of the @ref:[reference documentation](../actors.md), check out some @ref:[books and videos](../../additional/books.md), or even explore the free online courses @java[[**Akka Basics for Java**](https://akkademy.akka.io/learn/courses/23/akka-basics-for-java)]@scala[[**Akka Basics for Scala**](https://akkademy.akka.io/learn/courses/22/akka-basics-for-scala)].
* If you are interested in functional programming, read how actors can be defined in a @ref:[functional style](../actors.md#functional-style). In this guide the object-oriented style was used, but you can mix both as you like.

To get from this guide to a complete application you would likely need to provide either an UI or an API. For this we recommend that you look at the following technologies and see what fits you:

 * @extref[Microservices with Akka tutorial](platform-guide:microservices-tutorial/) illustrates how to implement an Event Sourced CQRS application with Akka Persistence and Akka Projections 
 * [Akka HTTP](https://doc.akka.io/libraries/akka-http/current/introduction.html) is a HTTP server and client library, making it possible to publish and consume HTTP endpoints
 * [Akka gRPC](https://doc.akka.io/libraries/akka-grpc/current/) supports fully typed, streaming gRPC servers and clients.
