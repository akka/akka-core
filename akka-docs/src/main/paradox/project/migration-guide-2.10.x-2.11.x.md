---
project.description: Migrating to Akka 2.11.
---
# Migration Guide 2.10.x to 2.11.x

## Jackson 2.21

@ref:[Akka Serialization Jackson](../serialization-jackson.md) now depends on the Jackson 2.21 long-term-support line. Akka 2.10 used Jackson 2.18 which will not get further updates past January 2027.

## Removed deprecated APIs

APIs deprecated in Akka 2.9 or earlier, or in Akka 2.10.0, have been removed. The sections below list the removed APIs and their replacements.

### akka-cluster-sharding-typed

* The deprecated `ClusterShardingSettings` constructors: use `ClusterShardingSettings(system)` or `ClusterShardingSettings.create(system)` with the `with*` methods, or the full constructor.
* `ClusterShardingSettings.passivateIdleEntityAfter` and `withPassivateIdleEntityAfter`: use `withPassivationStrategy` and `passivationStrategySettings`, see @ref:[Automatic Passivation](../typed/cluster-sharding.md).
* `ShardingProducerController.Settings.resendFirsUnconfirmedIdleTimeout` and `withResendFirsUnconfirmedIdleTimeout`: use the correctly spelled `resendFirstUnconfirmedIdleTimeout` and `withResendFirstUnconfirmedIdleTimeout`.
