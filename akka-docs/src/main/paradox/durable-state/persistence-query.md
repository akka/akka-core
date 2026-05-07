---
project.description: Query side to Akka Persistence allowing for building CQRS applications using durable state.
---
# Persistence Query

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Persistence Query, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-persistence-query_$scala.binary.version$
  version=AkkaVersion
}

This will also add dependency on the @ref[Akka Persistence](../persistence.md) module.

## Introduction

Akka persistence query provides a query interface to @ref:[Durable State Behaviors](../typed/durable-state/persistence.md).
These queries are based on asynchronous streams. These streams are similar to the ones offered in the @ref:[Event Sourcing](../persistence-query.md)
based implementation. Various state store plugins can implement these interfaces to expose their query capabilities.

One of the rationales behind having a separate query module for Akka Persistence is for implementing the so-called 
query side or read side in the popular CQRS architecture pattern - in which the writing side of the 
application implemented using Akka persistence, is completely separated from the query side.

@@@ note { title=Alternative }
When using the R2DBC plugin an alternative to using Akka persistence query or Projection is to
[store the query representation](https://doc.akka.io/libraries/akka-persistence-r2dbc/current/durable-state-store.html#storing-query-representation)
directly from the write side.
@@@

## Using query with Akka Projections

Akka Persistence and Akka Projections together can be used to develop a CQRS application. In the application the 
durable state is stored in a database and fetched as an asynchronous stream to the user. Currently queries on 
durable state, provided by the `DurableStateStoreQuery` interface, is used to implement tag based searches in 
Akka Projections. 

At present the query is based on _tags_. So if you have not tagged your objects, this query cannot be used.

The example below shows how to get the  `DurableStateStoreQuery` from the `DurableStateStoreRegistry` extension.

Scala
:  @@snip [DurableStateStoreQueryUsageCompileOnlySpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/DurableStateStoreQueryUsageCompileOnlySpec.scala) { #get-durable-state-store-query-example }

Java
:  @@snip [DurableStateStoreQueryUsageCompileOnlyTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/DurableStateStoreQueryUsageCompileOnlyTest.java) { #get-durable-state-store-query-example } 

The @apidoc[DurableStateChange] elements can be `UpdatedDurableState` or `DeletedDurableState`.
