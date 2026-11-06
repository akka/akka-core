# Persistence Plugins 

Storage backends for journals, snapshot stores, durable state stores and persistence queries are pluggable in the Akka persistence extension. The following plugins are maintained by the Akka team.

## R2DBC plugin

The Reactive database drivers (R2DBC) support relational databases like PostgreSQL, H2 (As a minimal in-process memory or file based database) and Yugabyte.

The [Akka Persistence R2DBC plugin](https://doc.akka.io/libraries/akka-persistence-r2dbc/current/) supports the latest feature additions of Akka Persistence and is generally recommended over the JDBC-based plugin.

## Cassandra plugin

Akka supports Cassandra's data model through [Akka Persistence Cassandra](https://doc.akka.io/libraries/akka-persistence-cassandra/current/).

Some later Akka Persistence feature additions (including @ref:[Durable State](./typed/index-persistence-durable-state.md)) are not supported by the Cassandra plugin (see below).

## AWS DynamoDB plugin

AWS DynamoDB can be used as backend for Akka Persistence with the [Akka Persistence DynamoDB plugin](https://doc.akka.io/libraries/akka-persistence-dynamodb/current/).

@ref:[Durable State](./typed/index-persistence-durable-state.md) is not supported by the DynamoDB plugin.

Recovery from only last event is not supported by the DynamoDB plugin.

## JDBC plugin

Relational databases with JDBC-drivers are supported through [Akka Persistence JDBC](https://doc.akka.io/libraries/akka-persistence-jdbc/current/). For new projects, the @ref:[R2DBC plugin](#r2dbc-plugin) is recommended.

Some later Akka Persistence feature additions are not supported by the Akka Persistence JDBC plugin (see below).

## Feature limitations

Example of concrete features _not_ supported by the Cassandra and JDBC plugins:

* `eventsBySlices` query
* Projections over gRPC
* Replicated Event Sourcing over gRPC
* Dynamic scaling of number of Projection instances
* Low latency Projections
* Projections starting from snapshots
* Scalability of many Projections
* Durable State entities (partly supported by JDBC plugin)
* Recovery from only last event

## Enabling a plugin

Plugins can be selected either by "default" for all persistent actors,
or "individually", when a persistent actor defines its own set of plugins.

When a persistent actor does NOT override the `journalPluginId` and `snapshotPluginId` methods,
the persistence extension will use the "default" journal, snapshot-store and durable-state plugins configured in `reference.conf`:

```
akka.persistence.journal.plugin = ""
akka.persistence.snapshot-store.plugin = ""
akka.persistence.state.plugin = ""
```

However, these entries are provided as empty "", and require explicit user configuration via override in the user `application.conf`.

* For an example of a snapshot store plugin which writes snapshots as individual files to the local filesystem see @ref:[Local snapshot store](#local-snapshot-store).
* The state store is relatively new, one available implementation is the [akka-persistence-jdbc-plugin](https://doc.akka.io/libraries/akka-persistence-jdbc/current/).

## Eager initialization of persistence plugin

By default, persistence plugins are started on-demand, as they are used. In some case, however, it might be beneficial
to start a certain plugin eagerly. In order to do that, you should first add `akka.persistence.Persistence`
under the `akka.extensions` key. Then, specify the IDs of plugins you wish to start automatically under
`akka.persistence.journal.auto-start-journals` and `akka.persistence.snapshot-store.auto-start-snapshot-stores`.

For example, if you want eager initialization for the inmem journal plugin and the local snapshot store plugin, your configuration should look like this:  

```
akka {

  extensions = [akka.persistence.Persistence]

  persistence {

    journal {
      plugin = "akka.persistence.journal.inmem"
      auto-start-journals = ["akka.persistence.journal.inmem"]
    }

    snapshot-store {
      plugin = "akka.persistence.snapshot-store.local"
      auto-start-snapshot-stores = ["akka.persistence.snapshot-store.local"]
    }

  }

}
```

## Pre-packaged plugins

The Akka Persistence module comes with few built-in persistence plugins, but none of these are suitable
for production usage in an Akka Cluster. 

### Local snapshot store

This plugin writes snapshot files to the local filesystem.

@@@ warning
The local snapshot store plugin cannot be used in an Akka Cluster since the storage is in a local file system.
@@@

The local snapshot store plugin config entry is `akka.persistence.snapshot-store.local`. 
Enable this plugin by defining config property:

@@snip [PersistencePluginDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistencePluginDocSpec.scala) { #local-snapshot-plugin-config }

The default storage location is a directory named `snapshots` in the current working
directory. This can be changed by configuration where the specified path can be relative or absolute:

@@snip [PersistencePluginDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistencePluginDocSpec.scala) { #snapshot-config }

Note that it is not mandatory to specify a snapshot store plugin. If you don't use snapshots
you don't have to configure it.

### Persistence Plugin Proxy

For testing purposes a persistence plugin proxy allows sharing of a journal and snapshot store on a single node across multiple 
actor systems (on the same or on different nodes). This, for example, allows persistent actors to failover to a backup 
node and continue using the shared journal instance from the backup node. The proxy works by forwarding all the 
journal/snapshot store messages to a single, shared, persistence plugin instance, and therefore supports any use case 
supported by the proxied plugin.

@@@ warning
A shared journal/snapshot store is a single point of failure and should only be used for testing
purposes.
@@@

The journal and snapshot store proxies are controlled via the `akka.persistence.journal.proxy` and
`akka.persistence.snapshot-store.proxy` configuration entries, respectively. Set the `target-journal-plugin` or
`target-snapshot-store-plugin` keys to the underlying plugin you wish to use (for example:
`akka.persistence.journal.inmem`). The `start-target-journal` and `start-target-snapshot-store` keys should be
set to `on` in exactly one actor system - this is the system that will instantiate the shared persistence plugin.
Next, the proxy needs to be told how to find the shared plugin. This can be done by setting the `target-journal-address`
and `target-snapshot-store-address` configuration keys, or programmatically by calling the
`PersistencePluginProxy.setTargetLocation` method.

@@@ note
Akka starts extensions lazily when they are required, and this includes the proxy. This means that in order for the
proxy to work, the persistence plugin on the target node must be instantiated. This can be done by instantiating the
`PersistencePluginProxyExtension` @ref:[extension](extending-akka.md), or by calling the `PersistencePluginProxy.start` method.
@@@

@@@ note
The proxied persistence plugin can (and should) be configured using its original configuration keys.
@@@
