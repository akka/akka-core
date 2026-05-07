---
project.description: Migrating to Akka 2.8.
---
# Migration Guide 2.7.x to 2.8.x

Akka 2.8.x is binary backwards compatible with 2.7.x with the ordinary exceptions listed in the
@ref:[Binary Compatibility Rules](../common/binary-compatibility-rules.md).

No configuration changes are needed for updating an application from Akka 2.7.x to 2.8.x.

Rolling updates of Akka Cluster from Akka 2.7.x to 2.8.x is fully supported.

A few deprecated features and OSGi has been removed in Akka 2.8.x, see sections below.

## OSGi no longer supported out of the box

OSGi packaging and support in Akka has been dropped.

Projects using OSGi will need to re-package or find another solution for using OSGi, you can find some possible hints
in issue: https://github.com/akka/akka-core/issues/28304

## Deprecated Classic Remoting has been removed.

Classic Remoting transport has been deprecated since Akka 2.6.0 (2019-11-06) and is replaced by the Artery transport,
which has been the default since 2.6.0 and declared ready for production in Akka 2.5.22 (2019-04-03).

See [migration guide for Akka 2.6.x](https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html#default-remoting-is-now-artery-tcp)
and [What is new in Artery](https://doc.akka.io/libraries/akka-core/2.6/remoting-artery.html#what-is-new-in-artery).

### Moved classes for Multi JVM TestKit

When using the @ref:[Multi JVM TestKit](../multi-jvm-testing.md) you need to change the imports for
`Direction` and `ThrottleMode` classes.

```
akka.remote.transport.ThrottlerTransportAdapter.*
```

was moved to:

```
akka.remote.testkit.*
```
