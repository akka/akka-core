# Classic Serialization

@@include[includes.md](includes.md) { #actor-api }

Serialization is the same for Classic and Typed actors. It is described in @ref:[Serialization](serialization.md),
aside from serialization of `ActorRef` that is described @ref:[here](#serializing-actorrefs).

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Serialization, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor_$scala.binary.version$"
  version=AkkaVersion
}

## Serializing ActorRefs

All ActorRefs are serializable when using @ref:[Serialization with Jackson](serialization-jackson.md),
but in case you are writing your own serializer, you might want to know how to serialize and deserialize them properly.
In the general case, the local address to be used depends on the type of remote
address which shall be the recipient of the serialized information. Use
`Serialization.serializedActorPath(actorRef)` like this:

Scala
:  @@snip [SerializationDocSpec.scala](/akka-docs/src/test/scala/docs/serialization/SerializationDocSpec.scala) { #imports }

Java
:  @@snip [SerializationDocTest.java](/akka-docs/src/test/java/jdocs/serialization/SerializationDocTest.java) { #imports }


Scala
:  @@snip [SerializationDocSpec.scala](/akka-docs/src/test/scala/docs/serialization/SerializationDocSpec.scala) { #actorref-serializer }

Java
:  @@snip [SerializationDocTest.java](/akka-docs/src/test/java/jdocs/serialization/SerializationDocTest.java) { #actorref-serializer }

This assumes that serialization happens in the context of sending a message
through the remote transport. There are other uses of serialization, though,
e.g. storing actor references outside of an actor application (database, etc.).
In this case, it is important to keep in mind that the
address part of an actor’s path determines how that actor is communicated with.
Storing a local actor path might be the right choice if the retrieval happens
in the same logical context, but it is not enough when deserializing it on a
different network host: for that it would need to include the system’s remote
transport address.

Scala
:  @@snip [SerializationDocSpec.scala](/akka-docs/src/test/scala/docs/serialization/SerializationDocSpec.scala) { #external-address-default }

Java
:  @@snip [SerializationDocTest.java](/akka-docs/src/test/java/jdocs/serialization/SerializationDocTest.java) { #external-address-default }

@@@ note

`ActorPath.toSerializationFormatWithAddress` differs from `toString` if the
address does not already have `host` and `port` components, i.e. it only
inserts address information for local addresses.

`toSerializationFormatWithAddress` also adds the unique id of the actor, which will
change when the actor is stopped and then created again with the same name.
Sending messages to a reference pointing the old actor will not be delivered
to the new actor. If you don't want this behavior, e.g. in case of long term
storage of the reference, you can use `toStringWithAddress`, which doesn't
include the unique id.

@@@

There is also a default remote address which is the one used by cluster support
(and typical systems have just this one); you can get it like this:

Scala
:  @@snip [SerializationDocSpec.scala](/akka-docs/src/test/scala/docs/serialization/SerializationDocSpec.scala) { #external-address-default }

Java
:  @@snip [SerializationDocTest.java](/akka-docs/src/test/java/jdocs/serialization/SerializationDocTest.java) { #external-address-default }

