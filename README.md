Akka
====

*Akka is a powerful platform that simplifies building and operating highly responsive, resilient, and scalable services.*

The platform consists of
* the [**Akka SDK**](https://doc.akka.io/java/index.html) for straightforward, rapid development with AI assist and automatic clustering. Services built with the Akka SDK are automatically clustered and can be deployed on any infrastructure.
* and [**Akka Automated Operations**](https://doc.akka.io/operations/akka-platform.html), a managed solution that handles everything for Akka SDK services from auto-elasticity to multi-region high availability running safely within your VPC.

The **Akka SDK** and **Akka Automated Operations** are built upon the foundational [**Akka libraries**](https://doc.akka.io/libraries/akka-dependencies/current/), providing the building blocks for distributed systems.

Akka has been downloaded more than 1 billion times and has powered thousands of systems for over 15 years.  Akka enables millions of concurrent users, terabyte stream processing, low-latency read/write data access, 99.9999% availability, and multi-region high availability and disaster recovery (HA/DR).

Akka is used to build agentic AI, AI inference, transactional, analytical, digital twin, IOT, and edge-to-cloud systems. It is simple to learn and operate. Akka creates production-ready systems without requiring prior knowledge of events, threading, or distributed systems.


Akka core library
=================

The Akka core library provides:

* Multi-threaded behavior without the use of low-level concurrency constructs like
  atomics or locks &#8212; relieving you from even thinking about memory visibility issues.
* Transparent remote communication between systems and their components &#8212; relieving you from writing and maintaining difficult networking code.
* A clustered, high-availability architecture that is elastic, scales in or out, on demand &#8212; enabling you to deliver a truly reactive system.

At Akka's core is the actor model which provides a level of abstraction that makes it
easier to write correct concurrent, parallel and distributed systems. The actor
model spans the full set of Akka libraries, providing you with a consistent way
of understanding and using them. Thus, Akka offers a depth of integration that
you cannot achieve by picking libraries to solve individual problems and trying
to piece them together.

Using the Actor Model we raise the abstraction level and provide a better platform to build correct concurrent and scalable applications. This model is a perfect match for the principles laid out in the [Reactive Manifesto](https://www.reactivemanifesto.org/).

For resilience, we adopt the "Let it crash" model which the telecom industry has used with great success to build applications that self-heal and systems that never stop.

Akka actors also provide the abstraction for transparent distribution and the basis for truly scalable and fault-tolerant applications.

Reference Documentation
-----------------------

The current versions of all Akka libraries are listed on the [Akka Dependencies](https://doc.akka.io/libraries/akka-dependencies/current/) page. Releases of the Akka core libraries in this repository are listed on the [GitHub releases](https://github.com/akka/akka-core/releases) page.

The reference documentation for all Akka libraries is available via [doc.akka.io/libraries/](https://doc.akka.io/libraries/), details for the Akka core libraries
for [Scala](https://doc.akka.io/libraries/akka-core/current/?language=scala) and [Java](https://doc.akka.io/libraries/akka-core/current/?language=java).

The current versions of all Akka libraries are listed on the [Akka Dependencies](https://doc.akka.io/libraries/akka-dependencies/current/) page. Releases of the Akka core libraries in this repository are listed on the [GitHub releases](https://github.com/akka/akka-core/releases) page.

Contributing
------------
**Contributions are *very* welcome!**

If you see an issue that you'd like to see fixed, or want to shape out some ideas,
the best way to make it happen is to help out by submitting a pull request implementing it.
We welcome contributions from all, even you are not yet familiar with this project,
We are happy to get you started, and will guide you through the process once you've submitted your PR.

Refer to the [CONTRIBUTING.md](https://github.com/akka/akka-core/blob/main/CONTRIBUTING.md) file for more details about the workflow,
and general hints on how to prepare your pull request. You can also ask for clarifications or guidance in GitHub issues directly,
or in the akka/dev chat if a more real time communication would be of benefit.

License
-------
Akka is licensed under the Business Source License 1.1, please see the [Akka License FAQ](https://akka.io/bsl-license-faq).

Tests and documentation are under a separate license, see the LICENSE file in each documentation and test root directory for details.
