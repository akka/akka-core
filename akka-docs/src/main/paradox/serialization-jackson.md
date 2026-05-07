---
project.description: Serialization with Jackson for Akka.
---
# Serialization with Jackson

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Jackson Serialization, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-serialization-jackson_$scala.binary.version$"
  version=AkkaVersion
}

## Introduction

You find general concepts for Akka serialization in the @ref:[Serialization](serialization.md) section.
This section describes how to use the Jackson serializer for application specific messages and persistent
events and snapshots.

[Jackson](https://github.com/FasterXML/jackson) has support for both text based JSON and
binary formats.

In many cases ordinary classes can be serialized by Jackson without any additional hints, but sometimes
annotations are needed to specify how to convert the objects to JSON/bytes.

## Usage

To enable Jackson serialization for a class there needs to be a serialization binding for it or one of its super classes
in serialization-bindings configuration. 

You can use one of the two predefined marker @scala[traits]@java[interfaces]
`akka.serialization.jackson.JsonSerializable` or `akka.serialization.jackson.CborSerializable`.

Scala
:  @@snip [SerializationDocSpec.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/SerializationDocSpec.scala) { #marker-interface }

Java
:  @@snip [MySerializable.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/MySerializable.java) { #marker-interface }

If the pre-defined markers are not suitable for your project it is also possible to define your own marker 
@scala[trait]@java[interface] and let the messages @scala[extend]@java[implement] that. You will then have to 
add serialization binding configuration for your own marker in config, see @ref:[Serialization](serialization.md) for more details.

That is all that is needed for basic classes where Jackson understands the structure. A few cases that requires
annotations are described below.

@@@ note

Add the `-parameters` Java compiler option for usage by the [ParameterNamesModule](https://github.com/FasterXML/jackson-modules-java8/tree/master/parameter-names).
It reduces the need for some annotations.

@@@

## Security

For security reasons it is disallowed to bind the Jackson serializers to
open-ended types that might be a target for [serialization gadgets](https://cowtowncoder.medium.com/on-jackson-cves-dont-panic-here-is-what-you-need-to-know-54cd0d6e8062),
such as:

* @javadoc[java.lang.Object](java.lang.Object)
* @javadoc[java.io.Serializable](java.io.Serializable)
* @javadoc[java.lang.Comparable](java.lang.Comparable).

The deny list of possible serialization gadget classes defined by Jackson databind are checked
and disallowed for deserialization.

@@@ warning

Don't use @javadoc[@JsonTypeInfo(use = Id.CLASS)](com.fasterxml.jackson.annotation.JsonTypeInfo) or @javadoc[ObjectMapper.enableDefaultTyping](com.fasterxml.jackson.databind.ObjectMapper#enableDefaultTyping--) since that is a security risk
when using @ref:[polymorphic types](#polymorphic-types).

@@@

### Formats

The following formats are supported, and you select which one to use in the `serialization-bindings`
configuration as described above.

* `jackson-json` - ordinary text based JSON
* `jackson-cbor` - binary [CBOR data format](https://github.com/FasterXML/jackson-dataformats-binary/tree/master/cbor)

The binary format is more compact, with slightly better performance than the JSON format.

## Annotations

### Polymorphic types

A polymorphic type is when a certain base type has multiple alternative implementations. When nested fields or
collections are of polymorphic type the concrete implementations of the type must be listed with @javadoc[@JsonTypeInfo](com.fasterxml.jackson.annotation.JsonTypeInfo)
and @javadoc[@JsonSubTypes](com.fasterxml.jackson.annotation.JsonSubTypes) annotations.

Example:

Scala
:  @@snip [SerializationDocSpec.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/SerializationDocSpec.scala) { #polymorphism }

Java
:  @@snip [SerializationDocTest.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/SerializationDocTest.java) { #polymorphism }

If you haven't defined the annotations you will see an exception like this:

```
InvalidDefinitionException: Cannot construct instance of `...` (no Creators, like default construct, exist): abstract types either need to be mapped to concrete types, have custom deserializer, or contain additional type information
```

Note that this is not needed for a top level class, but for fields inside it. In this example `Animal` is
used inside of `Zoo`, which is sent as a message or persisted. If `Animal` was sent or persisted standalone
the annotations are not needed because then it is the concrete subclasses `Lion` or `Elephant` that are
serialized.

When specifying allowed subclasses with those annotations the class names will not be included in the serialized
representation and that is important for @ref:[preventing loading of malicious serialization gadgets](#security)
when deserializing.

@@@ warning

Don't use @javadoc[@JsonTypeInfo(use = Id.CLASS)](com.fasterxml.jackson.annotation.JsonTypeInfo) or @javadoc[ObjectMapper.enableDefaultTyping](com.fasterxml.jackson.databind.ObjectMapper#enableDefaultTyping--) since that is a security risk
when using polymorphic types.

@@@

@@@ div {.group-scala}

### ADT with trait and case object

It's common in Scala to use a sealed trait and case objects to represent enums. If the values are case classes
the @javadoc[@JsonSubTypes](com.fasterxml.jackson.annotation.JsonSubTypes) annotation as described above works, but if the values are case objects it will not.
The annotation requires a @javadoc[Class](java.lang.Class) and there is no way to define that in an annotation for a `case object`.

The easiest workaround is to define the case objects as case class without any field. 

Alternatively, you can define an intermediate trait for the case object and a custom deserializer for it. The example below builds on the previous `Animal` sample by adding a fictitious, single instance, new animal, an `Unicorn`. 

Scala
:  @@snip [SerializationDocSpec.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/SerializationDocSpec.scala) { #polymorphism-case-object }

The case object `Unicorn` can't be used in a @javadoc[@JsonSubTypes](com.fasterxml.jackson.annotation.JsonSubTypes) annotation, but its trait can. When serializing the case object we need to know which type tag to use, hence the @javadoc[@JsonTypeName](com.fasterxml.jackson.annotation.JsonTypeName) annotation on the object. When deserializing, Jackson will only know about the trait variant therefore we need a custom deserializer that returns the case object. 

On the other hand, if the ADT only has case objects, you can solve it by implementing a custom serialization for the enums. Annotate the `trait` with
@javadoc[@JsonSerialize](com.fasterxml.jackson.databind.annotation.JsonSerialize) and @javadoc[@JsonDeserialize](com.fasterxml.jackson.databind.annotation.JsonDeserialize) and implement the serialization with @javadoc[StdSerializer](com.fasterxml.jackson.databind.ser.std.StdSerializer) and
@javadoc[StdDeserializer](com.fasterxml.jackson.databind.deser.std.StdDeserializer).

Scala
:  @@snip [CustomAdtSerializer.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/CustomAdtSerializer.scala) { #adt-trait-object }

### Enumerations

Jackson support for Scala Enumerations defaults to serializing a `Value` as a `JsonObject` that includes a 
field with the `"value"` and a field with the `"type"` whose value is the FQCN of the enumeration. Jackson
includes the [`@JsonScalaEnumeration`](https://github.com/FasterXML/jackson-module-scala/wiki/Enumerations) to 
statically specify the type information to a field. When using the `@JsonScalaEnumeration` annotation the enumeration 
value is serialized as a JsonString.

Scala
:  @@snip [JacksonSerializerSpec.scala](/akka-serialization-jackson/src/test/scala/akka/serialization/jackson/JacksonSerializerSpec.scala) { #jackson-scala-enumeration }
    
@@@


## Schema Evolution

When using Event Sourcing, but also for rolling updates, schema evolution becomes an important aspect of
developing your application. The requirements as well as our own understanding of the business domain may
(and will) change over time.

The Jackson serializer provides a way to perform transformations of the JSON tree model during deserialization.
This is working in the same way for the textual and binary formats.

We will look at a few scenarios of how the classes may be evolved.

### Remove Field

Removing a field can be done without any migration code. The Jackson serializer will ignore properties that does
not exist in the class.

### Add Optional Field

Adding an optional field can be done without any migration code. The default value will be @scala[None]@java[`Optional.empty`].

Old class:

Scala
:  @@snip [ItemAdded.java](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v1/ItemAdded.scala) { #add-optional }

Java
:  @@snip [ItemAdded.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v1/ItemAdded.java) { #add-optional }


New class with a new optional `discount` property and a new `note` field with default value:

Scala
:  @@snip [ItemAdded.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v2a/ItemAdded.scala) { #add-optional }

Java
:  @@snip [ItemAdded.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v2a/ItemAdded.java) { #add-optional }

### Add Mandatory Field

Let's say we want to have a mandatory `discount` property without default value instead:

Scala
:  @@snip [ItemAdded.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v2b/ItemAdded.scala) { #add-mandatory }

Java
:  @@snip [ItemAdded.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v2b/ItemAdded.java) { #add-mandatory }

To add a new mandatory field we have to use a @apidoc[JacksonMigration] class and set the default value in the migration code.

This is how a migration class would look like for adding a `discount` field:

Scala
:  @@snip [ItemAddedMigration.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v2b/ItemAddedMigration.scala) { #add-mandatory }

Java
:  @@snip [ItemAddedMigration.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v2b/ItemAddedMigration.java) { #add-mandatory }

Override the @scala[@scaladoc[currentVersion](akka.serialization.jackson.JacksonMigration#currentVersion:Int)]@java[@javadoc[currentVersion()](akka.serialization.jackson.JacksonMigration#currentVersion())] method to define the version number of the current (latest) version. The first version,
when no migration was used, is always 1. Increase this version number whenever you perform a change that is not
backwards compatible without migration code.

Implement the transformation of the old JSON structure to the new JSON structure in the @apidoc[transform(fromVersion, jsonNode)](JacksonMigration) {scala="#transform(fromVersion:Int,json:com.fasterxml.jackson.databind.JsonNode):com.fasterxml.jackson.databind.JsonNode" java="#transform(int,com.fasterxml.jackson.databind.JsonNode)"} method.
The @javadoc[JsonNode](com.fasterxml.jackson.databind.JsonNode)
is mutable, so you can add and remove fields, or change values. Note that you have to cast to specific sub-classes
such as @javadoc[ObjectNode](com.fasterxml.jackson.databind.node.ObjectNode)
and @javadoc[ArrayNode](com.fasterxml.jackson.databind.node.ArrayNode)
to get access to mutators.

The migration class must be defined in configuration file:

@@snip [config](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/SerializationDocSpec.scala) { #migrations-conf }

The same thing could have been done for the `note` field, adding a default value of `""` in the `ItemAddedMigration`.

### Rename Field

Let's say that we want to rename the `productId` field to `itemId` in the previous example.

Scala
:  @@snip [ItemAdded.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v2c/ItemAdded.scala) { #rename }

Java
:  @@snip [ItemAdded.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v2c/ItemAdded.java) { #rename }

The migration code would look like:

Scala
:  @@snip [ItemAddedMigration.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v2c/ItemAddedMigration.scala) { #rename }

Java
:  @@snip [ItemAddedMigration.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v2c/ItemAddedMigration.java) { #rename }

### Structural Changes

In a similar way we can do arbitrary structural changes.

Old class:

Scala
:  @@snip [Customer.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v1/Customer.scala) { #structural }

Java
:  @@snip [Customer.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v1/Customer.java) { #structural }

New class:

Scala
:  @@snip [Customer.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v2a/Customer.scala) { #structural }

Java
:  @@snip [Customer.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v2a/Customer.java) { #structural }

with the `Address` class:

Scala
:  @@snip [Address.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v2a/Address.scala) { #structural }

Java
:  @@snip [Address.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v2a/Address.java) { #structural }

The migration code would look like:

Scala
:  @@snip [CustomerMigration.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v2a/CustomerMigration.scala) { #structural }

Java
:  @@snip [CustomerMigration.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v2a/CustomerMigration.java) { #structural }

### Rename Class

It is also possible to rename the class. For example, let's rename `OrderAdded` to `OrderPlaced`.

Old class:

Scala
:  @@snip [OrderAdded.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v1/OrderAdded.scala) { #rename-class }

Java
:  @@snip [OrderAdded.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v1/OrderAdded.java) { #rename-class }

New class:

Scala
:  @@snip [OrderPlaced.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v2a/OrderPlaced.scala) { #rename-class }

Java
:  @@snip [OrderPlaced.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v2a/OrderPlaced.java) { #rename-class }

The migration code would look like:

Scala
:  @@snip [OrderPlacedMigration.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v2a/OrderPlacedMigration.scala) { #rename-class }

Java
:  @@snip [OrderPlacedMigration.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v2a/OrderPlacedMigration.java) { #rename-class }

Note the override of the @apidoc[transformClassName(fromVersion, className)](JacksonMigration) {scala="#transformClassName(fromVersion:Int,className:String):String" java="#transformClassName(int,java.lang.String)"} method to define the new class name.

That type of migration must be configured with the old class name as key. The actual class can be removed.

@@snip [config](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/SerializationDocSpec.scala) { #migrations-conf-rename }

### Remove from serialization-bindings

When a class is not used for serialization anymore it can be removed from `serialization-bindings` but to still
allow deserialization it must then be listed in the `allowed-class-prefix` configuration. This is useful for example
during rolling update with serialization changes, or when reading old stored data. It can also be used
when changing from Jackson serializer to another serializer (e.g. Protobuf) and thereby changing the serialization
binding, but it should still be possible to deserialize old data with Jackson.

@@snip [config](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/SerializationDocSpec.scala) { #allowed-class-prefix }

It's a list of class names or prefixes of class names.

## Rolling updates

When doing a rolling update, for a period of time there are two different binaries running in production. If the schema
has evolved requiring a new schema version, the data serialized by the new binary will be unreadable from the old 
binary. This situation causes transient errors on the processes running the old binary. This service degradation is 
usually fine since the rolling update will eventually complete and all old processes will be replaced with the new 
binary. To avoid this service degradation you can also use forward-one support in your schema evolutions.

To complete a no-degradation rolling update, you need to make two deployments. First, deploy a new binary which can read 
the new schema but still uses the old schema. Then, deploy a second binary which serializes data using the new schema
and drops the downcasting code from the migration.  

Let's take, for example, the case above where we [renamed a field](#rename-field).

The starting schema is:

Scala
:  @@snip [ItemAdded.java](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v1/ItemAdded.scala) { #add-optional }

Java
:  @@snip [ItemAdded.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v1/ItemAdded.java) { #add-optional }

In a first deployment, we still don't make any change to the event class:

Scala
:  @@snip [ItemAdded.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v1/ItemAdded.scala) { #forward-one-rename }

Java
:  @@snip [ItemAdded.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v1/ItemAdded.java) { #forward-one-rename }

but we introduce a migration that can read the newer schema which is versioned `2`:

Scala
:  @@snip [ItemAddedMigration.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v1withv2/ItemAddedMigration.scala) { #forward-one-rename }

Java
:  @@snip [ItemAddedMigration.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v1withv2/ItemAddedMigration.java) { #forward-one-rename }

Once all running nodes have the new migration code which can read version `2` of `ItemAdded` we can proceed with the 
second step. So, we deploy the updated event:

Scala
:  @@snip [ItemAdded.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v2c/ItemAdded.scala) { #rename }

Java
:  @@snip [ItemAdded.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v2c/ItemAdded.java) { #rename }

and the final migration code which no longer needs forward-compatibility code:

Scala
:  @@snip [ItemAddedMigration.scala](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/v2c/ItemAddedMigration.scala) { #rename }

Java
:  @@snip [ItemAddedMigration.java](/akka-serialization-jackson/src/test/java/jdoc/akka/serialization/jackson/v2c/ItemAddedMigration.java) { #rename }



## Jackson Modules

The following Jackson modules are enabled by default:

@@snip [reference.conf](/akka-serialization-jackson/src/main/resources/reference.conf) { #jackson-modules }

You can amend the configuration `akka.serialization.jackson.jackson-modules` to enable other modules.

The [ParameterNamesModule](https://github.com/FasterXML/jackson-modules-java8/tree/master/parameter-names) requires that the `-parameters`
Java compiler option is enabled.

### Compression

JSON can be rather verbose and for large messages it can be beneficial to compress large payloads. For
the `jackson-json` binding the default configuration is:

@@snip [reference.conf](/akka-serialization-jackson/src/main/resources/reference.conf) { #compression }

Supported compression algorithms are: gzip, lz4. Use 'off' to disable compression.
Gzip is generally slower than lz4.
Messages larger than the `compress-larger-than` property are compressed.

Compression can be disabled by setting the `algorithm` property to `off`. It will still be able to decompress
payloads that were compressed when serialized, e.g. if this configuration is changed.

For the `jackson-cbor` and custom bindings other than `jackson-json` compression is by default disabled,
but can be enabled in the same way as the configuration shown above but replacing `jackson-json` with
the binding name (for example `jackson-cbor`).

## Using Akka Serialization for embedded types

For types that already have an Akka Serializer defined that are embedded in types serialized with Jackson the @apidoc[AkkaSerializationSerializer] and
@apidoc[AkkaSerializationDeserializer] can be used to Akka Serialization for individual fields. 

The serializer/deserializer are not enabled automatically. The @javadoc[@JsonSerialize](com.fasterxml.jackson.databind.annotation.JsonSerialize) and @javadoc[@JsonDeserialize](com.fasterxml.jackson.databind.annotation.JsonDeserialize) annotation needs to be added
to the fields containing the types to be serialized with Akka Serialization.

The type will be embedded as an object with the fields:

* serId - the serializer id
* serManifest - the manifest for the type
* payload - base64 encoded bytes 

## Additional configuration

### Configuration per binding

By default, the configuration for the Jackson serializers and their @javadoc[ObjectMapper](com.fasterxml.jackson.databind.ObjectMapper)s is defined in
the `akka.serialization.jackson` section. It is possible to override that configuration in a more
specific `akka.serialization.jackson.<binding name>` section.

@@snip [config](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/SerializationDocSpec.scala) { #specific-config }

It's also possible to define several bindings and use different configuration for them. For example,
different settings for remote messages and persisted events.

@@snip [config](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/SerializationDocSpec.scala) { #several-config }

### Manifest-less serialization

When using the Jackson serializer for persistence, given that the fully qualified class name is
stored in the manifest, this can result in a lot of wasted disk and IO used, especially when the
events are small. To address this, a `type-in-manifest` flag can be turned off, which will result
in the class name not appearing in the manifest.

When deserializing, the Jackson serializer will use the type defined in `deserialization-type`, if
present, otherwise it will look for exactly one serialization binding class, and use that. For
this to be useful, generally that single type must be a 
@ref:[Polymorphic type](#polymorphic-types), with all type information necessary to deserialize to
the various sub types contained in the JSON message.

When switching serializers, for example, if doing a rolling update as described
@ref:[here](additional/rolling-updates.md#from-java-serialization-to-jackson), there will be
periods of time when you may have no serialization bindings declared for the type. In such
circumstances, you must use the `deserialization-type` configuration attribute to specify which
type should be used to deserialize messages.

Since this configuration can only be applied to a single root type, you will usually only want to
apply it to a per binding configuration, not to the regular `jackson-json` or `jackson-cbor`
configurations.

@@snip [config](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/SerializationDocSpec.scala) { #manifestless }

Note that Akka remoting already implements manifest compression, and so this optimization will have
no significant impact for messages sent over remoting. It's only useful for messages serialized for
other purposes, such as persistence or distributed data.

## Additional features

Additional Jackson serialization features can be enabled/disabled in configuration. The default values from
Jackson are used aside from the following that are changed in Akka's default configuration.

@@snip [reference.conf](/akka-serialization-jackson/src/main/resources/reference.conf) { #features }

### Date/time format

@javadoc[WRITE_DATES_AS_TIMESTAMPS](com.fasterxml.jackson.databind.SerializationFeature#WRITE_DATES_AS_TIMESTAMPS) and @javadoc[WRITE_DURATIONS_AS_TIMESTAMPS](com.fasterxml.jackson.databind.SerializationFeature#WRITE_DURATIONS_AS_TIMESTAMPS) are by default disabled, which means that date/time fields are serialized in
ISO-8601 (rfc3339) `yyyy-MM-dd'T'HH:mm:ss.SSSZ` format instead of numeric arrays. This is better for
interoperability but it is slower. If you don't need the ISO format for interoperability with external systems
you can change the following configuration for better performance of date/time fields.

@@snip [config](/akka-serialization-jackson/src/test/scala/doc/akka/serialization/jackson/SerializationDocSpec.scala) { #date-time }

Jackson is still able to deserialize the other format independent of this setting.
