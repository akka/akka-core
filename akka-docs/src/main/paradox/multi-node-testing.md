---
project.description: Multi node testing of distributed systems built with Akka.
---
# Multi Node Testing

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Multi Node Testing, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-multi-node-testkit_$scala.binary.version$
  version=AkkaVersion
  scope=test
}

@@project-info{ projectId="akka-multi-node-testkit" }

## Multi Node Testing Concepts

When we talk about multi node testing in Akka we mean the process of running coordinated tests on multiple actor
systems in different JVMs. The multi node testing kit consist of three main parts.

 * @ref:[The Test Conductor](#the-test-conductor). that coordinates and controls the nodes under test.
 * @ref:[The Multi Node Spec](#the-multi-node-spec). that is a convenience wrapper for starting the @apidoc[TestConductor$] and letting all
nodes connect to it.
 * @ref:[The SbtMultiJvm Plugin](#the-sbtmultijvm-plugin). that starts tests in multiple JVMs possibly on multiple machines.

## The Test Conductor

The basis for the multi node testing is the @apidoc[TestConductor$]. It is an Akka Extension that plugs in to the
network stack and it is used to coordinate the nodes participating in the test and provides several features
including:

 * Node Address Lookup: Finding out the full path to another test node (No need to share configuration between
test nodes)
 * Node Barrier Coordination: Waiting for other nodes at named barriers.
 * Network Failure Injection: Throttling traffic, dropping packets, unplugging and plugging nodes back in.

This is a schematic overview of the test conductor.

![akka-remote-testconductor.png](./images/akka-remote-testconductor.png)

The test conductor server is responsible for coordinating barriers and sending commands to the test conductor
clients that act upon them, e.g. throttling network traffic to/from another client. More information on the
possible operations is available in the @apidoc[akka.remote.testconductor.Conductor](Conductor) API documentation.

## The Multi Node Spec

The Multi Node Spec consists of two parts. The @apidoc[MultiNodeConfig] that is responsible for common
configuration and enumerating and naming the nodes under test. The `MultiNodeSpec` that contains a number
of convenience functions for making the test nodes interact with each other. More information on the possible
operations is available in the @apidoc[akka.remote.testkit.MultiNodeSpec](MultiNodeSpec) API documentation.

The setup of the `MultiNodeSpec` is configured through java system properties that you set on all JVMs that's going to run a
node under test. These can be set on the JVM command line with `-Dproperty=value`.

These are the available properties:
: 
 * 
   `multinode.max-nodes`
   The maximum number of nodes that a test can have.
 * 
   `multinode.host`
   The host name or IP for this node. Must be resolvable using InetAddress.getByName.
 * 
   `multinode.port`
   The port number for this node. Defaults to 0 which will use a random port.
 * 
   `multinode.server-host`
   The host name or IP for the server node. Must be resolvable using InetAddress.getByName.
 * 
   `multinode.server-port`
   The port number for the server node. Defaults to 4711.
 * 
   `multinode.index`
   The index of this node in the sequence of roles defined for the test. The index 0 is special and that machine
will be the server. All failure injection and throttling must be done from this node.


## The SbtMultiJvm Plugin

The @ref:[SbtMultiJvm Plugin](multi-jvm-testing.md) has been updated to be able to run multi node tests, by
automatically generating the relevant `multinode.*` properties. This means that you can run multi node tests
on a single machine without any special configuration by running them as normal multi-jvm tests. These tests can
then be run distributed over multiple machines without any changes by using the multi-node additions to the
plugin.

### Multi Node Specific Additions

The plugin also has a number of new `multi-node-*` sbt tasks and settings to support running tests on multiple
machines. The necessary test classes and dependencies are packaged for distribution to other machines with
[SbtAssembly](https://github.com/sbt/sbt-assembly) into a jar file with a name on the format
`<projectName>_<scalaVersion>-<projectVersion>-multi-jvm-assembly.jar`

@@@ note

To be able to distribute and kick off the tests on multiple machines, it is assumed that both host and target
systems are POSIX like systems with `ssh` and `rsync` available.

@@@

These are the available sbt multi-node settings:
: 
 * 
   `multiNodeHosts`
   A sequence of hosts to use for running the test, on the form `user@host:java` where host is the only required
part. Will override settings from file.
 * 
   `multiNodeHostsFileName`
   A file to use for reading in the hosts to use for running the test. One per line on the same format as above.
Defaults to `multi-node-test.hosts` in the base project directory.
 * 
   `multiNodeTargetDirName`
   A name for the directory on the target machine, where to copy the jar file. Defaults to `multi-node-test` in
the base directory of the ssh user used to rsync the jar file.
 * 
   `multiNodeJavaName`
   The name of the default Java executable on the target machines. Defaults to `java`.

Here are some examples of how you define hosts:
: 
 * 
   `localhost`
   The current user on localhost using the default java.
 * 
   `user1@host1`
   User `user1` on host `host1` with the default java.
 * 
   `user2@host2:/usr/lib/jvm/java-7-openjdk-amd64/bin/java`
   User `user2` on host `host2` using java 7.
 * 
   `host3:/usr/lib/jvm/java-6-openjdk-amd64/bin/java`
   The current user on host `host3` using java 6.


### Running the Multi Node Tests

To run all the multi node test in multi-node mode (i.e. distributing the jar files and kicking off the tests
remotely) from inside sbt, use the `multiNodeTest` task:

```none
multiNodeTest
```

To run all of them in multi-jvm mode (i.e. all JVMs on the local machine) do:

```none
multi-jvm:test
```

To run individual tests use the `multiNodeTestOnly` task:

```none
multiNodeTestOnly your.MultiNodeTest
```

To run individual tests in the multi-jvm mode do:

```none
multi-jvm:testOnly your.MultiNodeTest
```

More than one test name can be listed to run multiple specific tests. Tab completion in sbt makes it easy to
complete the test names.

## A Multi Node Testing Example

First we need some scaffolding to hook up the @apidoc[MultiNodeSpec] with your favorite test framework. Lets define a trait
`STMultiNodeSpec` that uses ScalaTest to start and stop `MultiNodeSpec`.

@@snip [STMultiNodeSpec.scala](/akka-remote-tests/src/test/scala/akka/remote/testkit/STMultiNodeSpec.scala) { #example }

Then we need to define a configuration. Lets use two nodes `"node1` and `"node2"` and call it
`MultiNodeSampleConfig`.

@@snip [MultiNodeSample.scala](/akka-remote-tests/src/multi-jvm/scala/akka/remote/sample/MultiNodeSample.scala) { #package #config }

And then finally to the node test code. That starts the two nodes, and demonstrates a barrier, and a remote actor
message send/receive.

@@snip [MultiNodeSample.scala](/akka-remote-tests/src/multi-jvm/scala/akka/remote/sample/MultiNodeSample.scala) { #package #spec }

## Things to Keep in Mind

There are a couple of things to keep in mind when writing multi node tests or else your tests might behave in
surprising ways.

 * Don't issue a shutdown of the first node. The first node is the controller and if it shuts down your test will break.
 * To be able to use `blackhole`, `passThrough`, and `throttle` you must activate the failure injector and
throttler transport adapters by specifying @scala[@scaladoc[testTransport(on = true)](akka.remote.testkit.MultiNodeConfig#testTransport(on:Boolean):Unit)]@java[@javadoc[testTransport(true)](akka.remote.testkit.MultiNodeConfig#testTransport(boolean))] in your `MultiNodeConfig`.
 * Throttling, shutdown and other failure injections can only be done from the first node, which again is the controller.
 * Don't ask for the address of a node using `node(address)` after the node has been shut down. Grab the address before
shutting down the node.
 * Don't use MultiNodeSpec methods like address lookup, barrier entry et.c. from other threads than the main test
thread. This also means that you shouldn't use them from inside an actor, a future, or a scheduled task.

## Configuration

There are several configuration properties for the Multi-Node Testing module, please refer
to the @ref:[reference configuration](general/configuration-reference.md#config-akka-multi-node-testkit).
