## Finite State Machine in Actors

This sample is an adaptation of [Dining Hakkers](http://www.dalnefre.com/wp/2010/08/dining-philosophers-in-humus/). 

To try this example locally, download the sources files with [akka-samples-fsm-java.zip](https://doc.akka.io/libraries/akka-core/current//attachments/akka-samples-fsm-java.zip).

Open [DiningHakkersTyped.scala](src/main/java/sample/DiningHakkers.java).

It illustrates how the behaviors and transitions can be defined with Akka Typed.

Start the application by typing `mvn compile exec:java -Dexec.mainClass="sample.DiningHakkers"`. In the log output you can see the actions of the `Hakker` actors.

Read more about Akka Typed in [the documentation](http://doc.akka.io/libraries/akka-core/current/).

---

The Akka family of projects is managed by teams at Lightbend with help from the community.

License
-------

Akka is licensed under the Business Source License 1.1, please see the [Akka License FAQ](https://www.lightbend.com/akka/license-faq).
