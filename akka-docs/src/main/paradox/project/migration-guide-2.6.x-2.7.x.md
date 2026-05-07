---
project.description: Migrating to Akka 2.7.
---
# Migration Guide 2.6.x to 2.7.x

The license for using Akka in production has been changed to Business Source License v1.1.
[Why We Are Changing the License for Akka](https://akka.io/blog//why-we-are-changing-the-license-for-akka)
explains the reasons and a [detailed FAQ](https://akka.io/bsl-license-faq) is available to answer many of the questions that you may have about the license change.

Akka 2.7.x is binary backwards compatible with 2.6.x with the ordinary exceptions listed in the
@ref:[Binary Compatibility Rules](../common/binary-compatibility-rules.md).

No configuration changes are needed for updating an application from Akka 2.6.x to 2.7.x.

Rolling updates of Akka Cluster from Akka 2.6.x to 2.7.x is fully supported.

No deprecated features or APIs have been removed in Akka 2.7.x.

## Dependency updates

### Jackson

The Jackson dependency for @ref:[Serialization with Jackson](../serialization-jackson.md) has been updated to 2.13.4
in Akka 2.7.0. That bump includes many fixes and changes to Jackson, but it should not introduce any incompatibility
in serialized format.

## Default configuration changes

### Persistence plugin-dispatcher

The default `plugin-dispatcher` for Akka Persistence plugins has been changed to use the ordinary
`akka.actor.default-dispatcher`.

Previously it used a `PinnedDispatcher`, which wasn't a good default choice and most plugins have already
overridden that setting.

## Akka Diagnostics

The extension "Akka Enhancements: Diagnostics Recorder" with the major version `1.x` is only compatible up until `2.6.x`.

For Akka 2.7 and beyond use `2.x` of `akka-diagnostics` which is now maintained under [Akka Diagnostics](https://doc.akka.io/libraries/akka-diagnostics/current/index.html).
