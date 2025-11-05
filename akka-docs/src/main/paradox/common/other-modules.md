# Other Akka libraries

This page describes modules that compliment libraries from the Akka core.  See [this overview](https://doc.akka.io/libraries/akka-core/current/typed/guide/modules.html) instead for a guide on the core library modules.

All Akka libraries and their current versions are listed at **[Akka libraries versions](https://doc.akka.io/libraries/akka-dependencies/current/)**.

Please find the **Akka SDK** documentation at [doc.akka.io](https://doc.akka.io).

## [Akka HTTP](https://doc.akka.io/libraries/akka-http/current/)

A full server- and client-side HTTP stack on top of akka-actor and akka-stream.

## [Akka gRPC](https://doc.akka.io/libraries/akka-grpc/current/)

Akka gRPC provides support for building streaming gRPC servers and clients on top of Akka Streams.

## [Alpakka](https://doc.akka.io/libraries/alpakka/current/)

Alpakka is a Reactive Enterprise Integration library for Java and Scala, based on Reactive Streams and Akka.

## [Alpakka Kafka Connector](https://doc.akka.io/libraries/alpakka-kafka/current/)

The Alpakka Kafka Connector connects Apache Kafka with Akka Streams.


## @extref[Akka Projections](akka-projection:)

Akka Projections let you process a stream of events or records from a source to a projected model or external system.


## [Amazon DynamoDB Plugin for Akka Persistence](https://doc.akka.io/libraries/akka-persistence-dynamodb/current/)

An Akka Persistence journal and snapshot store backed by Amazon's DynamoDB.


## [Cassandra Plugin for Akka Persistence](https://doc.akka.io/libraries/akka-persistence-cassandra/current/)

An Akka Persistence journal and snapshot store backed by Apache Cassandra.


## [JDBC Plugin for Akka Persistence](https://doc.akka.io/libraries/akka-persistence-jdbc/current/)

An Akka Persistence journal and snapshot store for use with JDBC-compatible databases. This implementation relies on [Slick](https://scala-slick.org/).

## [R2DBC Plugin for Akka Persistence](https://doc.akka.io/libraries/akka-persistence-r2dbc/current/)

An Akka Persistence journal and snapshot store for use with R2DBC-compatible databases. This implementation relies on [R2DBC](https://r2dbc.io/).

## Akka Management

* @extref:[Akka Management](akka-management:) provides a central HTTP endpoint for Akka management extensions.
* @extref:[Akka Cluster Bootstrap](akka-management:bootstrap/) helps bootstrapping an Akka cluster using Akka Discovery.
* @extref:[Akka Management Kubernetes Rolling Updates](akka-management:rolling-updates.html) for smooth rolling updates.
* @extref:[Akka Management Cluster HTTP](akka-management:cluster-http-management.html) provides HTTP endpoints for introspecting and managing Akka clusters.
* @extref:[Akka Discovery for Kubernetes, Consul, Marathon, and AWS](akka-management:discovery/)
* @extref:[Kubernetes Lease](akka-management:kubernetes-lease.html)

## Akka Diagnostics

* [Akka Thread Starvation Detector](https://doc.akka.io/libraries/akka-diagnostics/current/starvation-detector.html)
* [Akka Configuration Checker](https://doc.akka.io/libraries/akka-diagnostics/current/config-checker.html)

## Akka Insights

Intelligent monitoring and observability purpose-built for Akka: [Akka Insights](https://doc.akka.io/libraries/akka-insights/current/home.html)
