# Rolling Updates and Versions

## Akka upgrades
Akka supports rolling updates between two consecutive patch versions unless an exception is
mentioned on this page. For example updating Akka version from 2.5.15 to 2.5.16. Many times
it is also possible to skip several versions and exceptions to that are also described here.
For example it's possible to update from 2.5.14 to 2.5.16 without intermediate 2.5.15.

It's not supported to have a cluster with more than two different versions. Roll out the first
update completely before starting next update.

@@@ note

@ref:[Rolling update from classic remoting to Artery](../additional/rolling-updates.md#migrating-from-classic-remoting-to-artery) is not supported since the protocol
is completely different. It will require a full cluster shutdown and new startup.

@@@

## Change log

### 2.5.0 Several changes in minor release

See [migration guide](https://doc.akka.io/libraries/akka-core/2.5/project/migration-guide-2.4.x-2.5.x.html#rolling-update) when updating from 2.4.x to 2.5.x.

### 2.5.10 Joining regression

Issue: [#24622](https://github.com/akka/akka-core/issues/24622)

Incompatible change was introduced in 2.5.10 and fixed in 2.5.11.

This means that you can't do a rolling update from 2.5.9 to 2.5.10 and must instead do update from 2.5.9 to 2.5.11.

### 2.5.10 Joining old versions

Issue: [#25491](https://github.com/akka/akka-core/issues/25491)

Incompatibility was introduced in in 2.5.10 and fixed in 2.5.15.

That means that you should do rolling update from 2.5.9 directly to 2.5.15 if you need to be able to
join 2.5.9 nodes during the update phase.

### 2.5.14 Distributed Data serializer for `ORSet[ActorRef]`

Issue: [#23703](https://github.com/akka/akka-core/issues/23703)

Intentional change was done in 2.5.14.

This change required a two phase update where the data was duplicated to be compatible with both old and new nodes.

* 2.5.13 - old format, before the change. Can communicate with intermediate format and with old format.
* 2.5.14, 2.5.15, 2.5.16 - intermediate format. Can communicate with old format and with new format.
* 2.5.17 - new format. Can communicate with intermediate format and with new format.

This means that you can't update from 2.5.13 directly to 2.5.17. You must first update to one of the intermediate
versions 2.5.14, 2.5.15, or 2.5.16.

### 2.5.22 ClusterSharding serializer for `ShardRegionStats`

Issue: [#25348](https://github.com/akka/akka-core/issues/25348)

Intentional change was done in 2.5.22.

Changed serializer for classes: `GetShardRegionStats`, `ShardRegionStats`, `GetShardStats`, `ShardStats`

This change required a two phase update where new serializer was introduced but not enabled in an earlier version.

* 2.5.18 - serializer was added but not enabled, `JavaSerializer` still used
* 2.5.22 - `ClusterShardingMessageSerializer` was enabled for these classes

This means that you can't update from 2.5.17 directly to 2.5.22. You must first update to one of the intermediate
versions 2.5.18, 2.5.19, 2.5.20 or 2.5.21.

### 2.6.0 Several changes in minor release

See @ref:[migration guide](migration-guide-2.5.x-2.6.x.md) when updating from 2.5.x to 2.6.x.

### 2.6.2 ClusterMessageSerializer manifests change

Issue: [#23654](https://github.com/akka/akka-core/issues/13654)

In preparation of switching away from class based manifests to more efficient letter codes the `ClusterMessageSerializer`
has been prepared to accept those shorter forms but still emits the old long manifests.

* 2.6.2 - shorter manifests accepted
* 2.6.5 - shorter manifests emitted

This means that a rolling update will have to go through at least one of 2.6.2, 2.6.3 or 2.6.4 when upgrading to
2.6.5 or higher or else cluster nodes will not be able to communicate during the rolling update.

### 2.6.5 JacksonCborSerializer

Issue: [#28918](https://github.com/akka/akka-core/issues/28918). JacksonCborSerializer was using plain JSON format
instead of CBOR.

If you have `jackson-cbor` in your `serialization-bindings` a rolling update will have to go through 2.6.5 when
upgrading to 2.6.5 or higher.

In Akka 2.6.5 the `jackson-cbor` binding will still serialize to JSON format to support rolling update from 2.6.4.
It also adds a new binding to be able to deserialize CBOR format when rolling update from 2.6.5 to 2.6.6.
In Akka 2.6.6 the `jackson-cbor` binding will serialize to CBOR and that can be deserialized by 2.6.5. Old
data, such as persistent events, can still be deserialized.

You can start using CBOR format already with Akka 2.6.5 without waiting for the 2.6.6 release. First, perform
a rolling update to Akka 2.6.5 using default configuration. Then change the configuration to:

```
akka.actor {
  serializers {
    jackson-cbor = "akka.serialization.jackson.JacksonCborSerializer"
  }
  serialization-identifiers {
    jackson-cbor = 33
  }
}
```

Perform a second rolling update with the new configuration.
