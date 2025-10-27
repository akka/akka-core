---
project.description: How to build native image executables with Akka
---
# Building Native Images

Building native images with Akka is supported, both for local and cluster actor system applications. 

Most built-in features in Akka can be used as is, however some functionality is not available and some extension points
require additional metadata for reflective access to work. Akka and the Akka umbrella tries to provide metadata for third
party dependencies where needed and not provided by the dependency to allow applications with zero or little extra 
native-image metadata.

A full sample including many of the Akka projects and build tool set up can be found in the @extref[Akka Edge Documentation](akka-edge:lightweight-deployments.html#graalvm-native-image). 

## Unsupported features

The following features and aspects are not currently supported out of the box:

* Lightbend Telemetry
* Aeron UDP remoting
* Testkits
* LevelDB and InMem Akka Persistence plugins
* @ref[Durable storage](../typed/distributed-data.md#durable-storage) for Akka Distributed Data
* Scala 3

## Features requiring additional metadata

In general, any feature that allows a custom implementation provided by a third party library or user code that is
then plugged into Akka through an entry in the application config file needs to have reflection metadata explicitly added
as described in the [GraalVM docs here](https://www.graalvm.org/latest/reference-manual/native-image/metadata/).

Examples of such classes are:

 * Custom Serializers
 * Custom Mailbox Types
 * Akka Persistence plugins
 * Akka Discovery implementations
 * Akka Lease implementations

For plugins provided in libraries, metadata is preferably provided with the library so that end user applications
do not need to provide metadata for them.

### Serialization with Jackson

When using the built-in @apidoc[JsonSerializable] and @apidoc[CborSerializable] marker traits, message types are automatically added
for reflective access by Akka. But there are a few important caveats:

Messages with only primitives, standard library, your own or Akka provided types are automatically registered. More complex
message structures and special Jackson annotations must be carefully tested as it is hard to predict if and how 
Jackson will reflectively interact with them. 

Types only referenced in fields as type parameters of other generic types, like for example
in a collection @scala[List[MyClass]]@java[List<MyClass>], will require explicit marker traits to be picked up 
@scala[`class MyClass() extends JsonSerializable`] @java[`class MyClass implements JsonSerializable`] or explicit entries
in the `reflect-config.json` of your application.

@scala[Scala standard library enumerations are not supported for serialization out of the box.]

If self-defined marker traits are being used, then the marker trait defined in `serialization-bindings`, as well as each 
concrete message type (lookup, constructor fields as needed by Jackson) and the field types, need to be added to the 
reflection metadata.

Applications that define and configure @apidoc[JacksonMigration] to evolve data formats need to list each concrete
migration implementation (lookup and zero parameter constructor) in reflection metadata.

Additional object mappers added through config `akka.serialization.jackson.jackson-modules` need entries in the reflective 
metadata (lookup and constructor).

### Third party serializers

Third party serializers will need reflection metadata for the serializer implementation, the types used in `serialization-bindings`
and possibly further per-message metadata depending on the specific serializer logic.

### Extensions

Classic and typed `Extension`s loaded via configuration (`akka.extensions`, `akka.actor.typed.extensions`, `akka.actor.library-extensions` or `akka.actor.typed.library-extensions`)
need an entry in the reflection metadata (class and constructor).

### Akka Persistence Event Adapters

Event adapters defined in an application need to be listed in reflection metadata (class and constructor).

### Reflective classic actor construction

Classic actors that have a @apidoc[akka.actor.Props$] defined using the @scala[type `Props[T]()` or class `Props(classOf[T], ...)`] 
@java[class `Props.create(T.getClass, ...)`] need reflection entries for lookup and the constructor matching the set of passed parameters. 

An easier path is to instead use lambda factories @scala[type `Props(new T)`]
@java[class `Props.create(T.getClass, () -> new T())`] to define props. The only reason to use the reflection based approach
is the classic remote deploy feature.

### Logging

When using `akka-slf4j` for logging, automatically used for `akka-actor-typed`, it is very likely that the concrete logger
chosen needs extra configuration.

While Akka does not mandate a logger implementation, `logback-classic` is used in many Akka samples throughout the Akka projects. 
Because of this Akka provides reflection metadata for logback out of the box, however projects using it will need an extra
native image flag `--initialize-at-build-time=ch.qos.logback`.

Special care needs to be taken if using the async logback appender. Either avoid using
`ch.qos.logback.classic.AsyncAppender` or declare your own lazy version of the appender (not starting any threads at native image build time). You can see an example of such a lazy appender in the @scala[[Akka Projections edge replication sample](https://github.com/akka/akka-projection/blob/main/samples/grpc/local-drone-control-scala/src/main/scala/local/logback/NativeImageAsyncAppender.scala)]@java[[Akka Projections edge replication sample](https://github.com/akka/akka-projection/blob/main/samples/grpc/local-drone-control-java/src/main/java/local/logback/NativeImageAsyncAppender.java)]
