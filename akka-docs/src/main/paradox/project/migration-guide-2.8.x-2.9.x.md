---
project.description: Migrating to Akka 2.9.
---
# Migration Guide 2.8.x to 2.9.x

Akka 2.9.x is binary backwards compatible with 2.8.x with the ordinary exceptions listed in the
@ref:[Binary Compatibility Rules](../common/binary-compatibility-rules.md).

No configuration changes are needed for updating an application from Akka 2.8.x to 2.9.x.

Rolling updates of Akka Cluster from Akka 2.8.x to 2.9.x is fully supported.

A few deprecated features has been removed in Akka 2.9.x, see sections below.

## Akka repository

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

## Support for Java 8 removed

The published artifacts are targeting Java 11, and later. Supported Java versions are 11 and 17.

## Support for Scala 2.12 removed

The published artifacts are targeting Scala 2.13 and Scala 3.3.

## Deprecated Cluster Client removed

Cluster client has been deprecated since Akka 2.6.0 (2019-11-06). Details on how to migrate can be found in the Akka 
2.6 docs here: https://doc.akka.io/libraries/akka-core/2.6/cluster-client.html#migration-to-akka-grpc


## Deprecated Typed Actor removed

The old "Typed Actor" API (`akka.actor.TypedActor`) has been deprecated since Akka 2.6.0 (2019-11-06) and has been dropped.
No detailed migration guide exists, the recommendation is to move to the new Akka Typed APIs. 

## Deprecated Akka SSLConfig removed

The Akka SSLConfig convenience and methods accepting it has been deprecated since Akka 2.6.0 and has been dropped. 
Usage should be replaced with directly creating a `javax.net.ssl.SSLEngine` using the JDK APIs.

The dependency to `ssl-config-core` has been removed. If you need ssl-config for other reasons, such as running
with older versions of Akka HTTP you can add the dependency to your build:

@@dependency [Maven,sbt,Gradle] {
group=com.typesafe
artifact=ssl-config-core_$scala.binary.version$
version="0.6.1"
}

## Persistent FSM

Persistent FSM has been deprecated since Akka 2.6.0 (2019-11-06) and has now been dropped. Details on how to migrate can be found in the Akka 2.8
docs here: https://doc.akka.io/libraries/akka-core/2.8/persistence-fsm.html#migration-to-eventsourcedbehavior

# Migration Guide 2.9.2 to 2.9.3

Named circuit breaker lookup was taking a `ExtendedActorSystem` requiring a typecast, is now requiring a `ClassicActorSystemProvider`.
This change is source compatible, but not binary compatible, so will require a recompile if you are using named circuit breakers.
