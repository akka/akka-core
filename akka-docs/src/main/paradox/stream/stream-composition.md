# Modularity, Composition and Hierarchy

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Streams, add the module to your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-stream_$scala.binary.version$"
  version=AkkaVersion
}

## Introduction

Akka Streams provide a uniform model of stream processing graphs, which allows flexible composition of reusable
components. In this chapter we show how these look like from the conceptual and API perspective, demonstrating
the modularity aspects of the library.

## Basics of composition and modularity

Every operator used in Akka Streams can be imagined as a "box" with input and output ports where elements to
be processed arrive and leave the operator. In this view, a `Source` is nothing else than a "box" with a single
output port, or, a `BidiFlow` is a "box" with exactly two input and two output ports. In the figure below
we illustrate the most commonly used operators viewed as "boxes".

![compose_shapes.png](../images/compose_shapes.png)

The *linear* operators are `Source`, `Sink`
and `Flow`, as these can be used to compose strict chains of operators.
Fan-in and Fan-out operators have usually multiple input or multiple output ports, therefore they allow to build
more complex graph layouts, not only chains. `BidiFlow` operators are usually useful in IO related tasks, where
there are input and output channels to be handled. Due to the specific shape of `BidiFlow` it is easy to
stack them on top of each other to build a layered protocol for example. The `TLS` support in Akka is for example
implemented as a `BidiFlow`.

These reusable components already allow the creation of complex processing networks. What we
have seen so far does not implement modularity though. It is desirable for example to package up a larger graph entity into
a reusable component which hides its internals only exposing the ports that are meant to the users of the module
to interact with. One good example is the `Http` server component, which is encoded internally as a
`BidiFlow` which interfaces with the client TCP connection using an input-output port pair accepting and sending
`ByteString` s, while its upper ports emit and receive `HttpRequest` and `HttpResponse` instances.

The following figure demonstrates various composite operators, that contain various other type of operators internally, but
hiding them behind a *shape* that looks like a `Source`, `Flow`, etc.

![compose_composites.png](../images/compose_composites.png)

One interesting example above is a `Flow` which is composed of a disconnected `Sink` and `Source`.
This can be achieved by using the `fromSinkAndSource()` constructor method on `Flow` which takes the two parts as
parameters.

Please note that when combining a `Flow` using that method, the termination signals are not carried 
"through" as the `Sink` and `Source` are assumed to be fully independent. If however you want to construct
a `Flow` like this but need the termination events to trigger "the other side" of the composite flow, you can use
`Flow.fromSinkAndSourceCoupled` or `Flow.fromSinkAndSourceCoupledMat` which does just that. For example the cancellation 
of the composite flows source-side will then lead to completion of its sink-side. Read @apidoc[Flow]'s API documentation for a 
detailed explanation how this works.

The example `BidiFlow` demonstrates that internally a module can be of arbitrary complexity, and the exposed
ports can be wired in flexible ways. The only constraint is that all the ports of enclosed modules must be either
connected to each other, or exposed as interface ports, and the number of such ports needs to match the requirement
of the shape, for example a `Source` allows only one exposed output port, the rest of the internal ports must
be properly connected.

These mechanics allow arbitrary nesting of modules. For example the following figure demonstrates a `RunnableGraph`
that is built from a composite `Source` and a composite `Sink` (which in turn contains a composite
`Flow`).

![compose_nested_flow.png](../images/compose_nested_flow.png)

The above diagram contains one more shape that we have not seen yet, which is called `RunnableGraph`. It turns
out, that if we wire all exposed ports together, so that no more open ports remain, we get a module that is *closed*.
This is what the `RunnableGraph` class represents. This is the shape that a `Materializer` can take
and turn into a network of running entities that perform the task described. In fact, a `RunnableGraph` is a
module itself, and (maybe somewhat surprisingly) it can be used as part of larger graphs. It is rarely useful to embed
a closed graph shape in a larger graph (since it becomes an isolated island as there are no open port for communication
with the rest of the graph), but this demonstrates the uniform underlying model.

If we try to build a code snippet that corresponds to the above diagram, our first try might look like this:

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #non-nested-flow }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #non-nested-flow }


It is clear however that there is no nesting present in our first attempt. Since the library cannot figure out
where we intended to put composite module boundaries, it is our responsibility to do that. If we are using the
DSL provided by the `Flow`, `Source`, `Sink` classes then nesting can be achieved by calling one of the
methods `withAttributes()` or `named()` (where the latter is a shorthand for adding a name attribute).

The following code demonstrates how to achieve the desired nesting:

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #nested-flow }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #nested-flow }

Once we have hidden the internals of our components, they act like any other built-in component of similar shape. If
we hide some of the internals of our composites, the result looks just like if any other predefine component has been
used:

![compose_nested_flow_opaque.png](../images/compose_nested_flow_opaque.png)

If we look at usage of built-in components, and our custom components, there is no difference in usage as the code
snippet below demonstrates.

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #reuse }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #reuse }

## Composing complex systems

In the previous section we explored the possibility of composition, and hierarchy, but we stayed away from non-linear,
generalized operators. There is nothing in Akka Streams though that enforces that stream processing layouts
can only be linear. The DSL for `Source` and friends is optimized for creating such linear chains, as they are
the most common in practice. There is a more advanced DSL for building complex graphs, that can be used if more
flexibility is needed. We will see that the difference between the two DSLs is only on the surface: the concepts they
operate on are uniform across all DSLs and fit together nicely.

As a first example, let's look at a more complex layout:

![compose_graph.png](../images/compose_graph.png)

The diagram shows a `RunnableGraph` (remember, if there are no unwired ports, the graph is closed, and therefore
can be materialized) that encapsulates a non-trivial stream processing network. It contains fan-in, fan-out operators,
directed and non-directed cycles. The `runnable()` method of the `GraphDSL` object allows the creation of a
general, closed, and runnable graph. For example the network on the diagram can be realized like this:

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #complex-graph }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #complex-graph }

In the code above we used the implicit port numbering feature (to make the graph more readable and similar to the diagram)
and we imported `Source` s, `Sink` s and `Flow` s explicitly. It is possible to refer to the ports
explicitly, and it is not necessary to import our linear operators via `add()`, so another version might look like this:

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #complex-graph-alt }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #complex-graph-alt }

Similar to the case in the first section, so far we have not considered modularity. We created a complex graph, but
the layout is flat, not modularized. We will modify our example, and create a reusable component with the graph DSL.
The way to do it is to use the `create()` factory method on `GraphDSL`. If we remove the sources and sinks
from the previous example, what remains is a partial graph:

![compose_graph_partial.png](../images/compose_graph_partial.png)

We can recreate a similar graph in code, using the DSL in a similar way than before:

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #partial-graph }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #partial-graph }

The only new addition is the return value of the builder block, which is a `Shape`. All operators (including
`Source`, `BidiFlow`, etc.) have a shape, which encodes the *typed* ports of the module. In our example
there is exactly one input and output port left, so we can declare it to have a `FlowShape` by returning an
instance of it. While it is possible to create new `Shape` types, it is usually recommended to use one of the
matching built-in ones.

The resulting graph is already a properly wrapped module, so there is no need to call *named()* to encapsulate the graph, but
it is a good practice to give names to modules to help debugging.

![compose_graph_shape.png](../images/compose_graph_shape.png)

Since our partial graph has the right shape, it can be already used in the simpler, linear DSL:

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #partial-use }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #partial-use }

It is not possible to use it as a `Flow` yet, though (i.e. we cannot call `.filter()` on it), but `Flow`
has a `fromGraph()` method that adds the DSL to a `FlowShape`. There are similar methods on `Source`,
`Sink` and `BidiShape`, so it is easy to get back to the simpler DSL if an operator has the right shape.
For convenience, it is also possible to skip the partial graph creation, and use one of the convenience creator methods.
To demonstrate this, we will create the following graph:

![compose_graph_flow.png](../images/compose_graph_flow.png)

The code version of the above closed graph might look like this:

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #partial-flow-dsl }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #partial-flow-dsl }

@@@ note

All graph builder sections check if the resulting graph has all ports connected except the exposed ones and will
throw an exception if this is violated.

@@@

We are still in debt of demonstrating that `RunnableGraph` is a component like any other, which can
be embedded in graphs. In the following snippet we embed one closed graph in another:

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #embed-closed }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #embed-closed }

The type of the imported module indicates that the imported module has a `ClosedShape`, and so we are not
able to wire it to anything else inside the enclosing closed graph. Nevertheless, this "island" is embedded properly,
and will be materialized just like any other module that is part of the graph.

As we have demonstrated, the two DSLs are fully interoperable, as they encode a similar nested structure of "boxes with
ports", it is only the DSLs that differ to be as much powerful as possible on the given abstraction level. It is possible
to embed complex graphs in the fluid DSL, and it is just as easy to import and embed a `Flow`, etc, in a larger,
complex structure.

We have also seen, that every module has a `Shape` (for example a `Sink` has a `SinkShape`)
independently which DSL was used to create it. This uniform representation enables the rich composability of various
stream processing entities in a convenient way.

## Materialized values

After realizing that `RunnableGraph` is nothing more than a module with no unused ports (it is an island), it becomes clear that
after materialization the only way to communicate with the running stream processing logic is via some side-channel.
This side channel is represented as a *materialized value*. The situation is similar to `Actor` s, where the
`Props` instance describes the actor logic, but it is the call to `actorOf()` that creates an actually running
actor, and returns an `ActorRef` that can be used to communicate with the running actor itself. Since the
`Props` can be reused, each call will return a different reference.

When it comes to streams, each materialization creates a new running network corresponding to the blueprint that was
encoded in the provided `RunnableGraph`. To be able to interact with the running network, each materialization
needs to return a different object that provides the necessary interaction capabilities. In other words, the
`RunnableGraph` can be seen as a factory, which creates:

 * a network of running processing entities, inaccessible from the outside
 * a materialized value, optionally providing a controlled interaction capability with the network

Unlike actors though, each of the operators might provide a materialized value, so when we compose multiple
operators or modules, we need to combine the materialized value as well (there are default rules which make this easier,
for example *to()* and *via()* takes care of the most common case of taking the materialized value to the left.
See @ref:[Combining materialized values](stream-flows-and-basics.md#flow-combine-mat) for details). 
We demonstrate how this works by a code example and a diagram which graphically demonstrates what is happening.

The propagation of the individual materialized values from the enclosed modules towards the top will look like this:

![compose_mat.png](../images/compose_mat.png)

To implement the above, first, we create a composite `Source`, where the enclosed `Source` have a
materialized type of @scala[`Promise[[Option[Int]]`] @java[`CompletableFuture<Optional<Integer>>>`]. By using the combiner function `Keep.left`, the resulting materialized
type is of the nested module (indicated by the color *red* on the diagram):

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #mat-combine-1 }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #mat-combine-1 }

Next, we create a composite `Flow` from two smaller components. Here, the second enclosed `Flow` has a
materialized type of @scala[`Future[OutgoingConnection]`] @java[`CompletionStage<OutgoingConnection>`], and we propagate this to the parent by using `Keep.right`
as the combiner function (indicated by the color *yellow* on the diagram):

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #mat-combine-2 }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #mat-combine-2 }

As a third step, we create a composite `Sink`, using our `nestedFlow` as a building block. In this snippet, both
the enclosed `Flow` and the folding `Sink` has a materialized value that is interesting for us, so
we use `Keep.both` to get a `Pair` of them as the materialized type of `nestedSink` (indicated by the color
*blue* on the diagram)

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #mat-combine-3 }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #mat-combine-3 }

As the last example, we wire together `nestedSource` and `nestedSink` and we use a custom combiner function to
create a yet another materialized type of the resulting `RunnableGraph`. This combiner function ignores
the @scala[`Future[String]`] @java[`CompletionStage<String>`] part, and wraps the other two values in a custom case class `MyClass`
(indicated by color *purple* on the diagram):

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #mat-combine-4 }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #mat-combine-4a }
    
    @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #mat-combine-4b }


@@@ note

The nested structure in the above example is not necessary for combining the materialized values, it 
demonstrates how the two features work together. See @ref:[Combining materialized values](stream-flows-and-basics.md#flow-combine-mat) for further examples
of combining materialized values without nesting and hierarchy involved.

@@@

## Attributes

We have seen that we can use `named()` to introduce a nesting level in the fluid DSL and also explicit nesting by using
`create()` from `GraphDSL`. Apart from having the effect of adding a nesting level, `named()` is actually
a shorthand for calling `addAttributes(Attributes.name("someName"))`, adding the `name` attribute to the graph. 

Attributes provide a way to fine-tune certain aspects of the materialized running entity. Attributes are inherited by 
nested modules, unless they override them with a custom value. This means the attribute specified closest to the operator 
in the graph will be the one that is in effect for that operator. 

Another example of an attribute is the `inputBuffer` attribute which has the main purpose to provide control over buffer sizes
for asynchronous boundaries (see @ref:[Buffers for asynchronous operators](stream-rate.md#async-stream-buffers)). 

The code below, a modification of an earlier example sets the `inputBuffer` attribute on certain modules, but not
on others. _Note_ that this is only to show how attributes inheritance works, the actual `inputBuffer` attribute does not
have any specific effect when running these streams:

Scala
:   @@snip [CompositionDocSpec.scala](/akka-docs/src/test/scala/docs/stream/CompositionDocSpec.scala) { #attributes-inheritance }

Java
:   @@snip [CompositionDocTest.java](/akka-docs/src/test/java/jdocs/stream/CompositionDocTest.java) { #attributes-inheritance }

The effect is, that each module inherits the `inputBuffer` attribute from its enclosing parent, unless it has
the same attribute explicitly set. `nestedSource` gets the default attributes from the materializer itself. `nestedSink`
on the other hand has this attribute set, so it will be used by all nested modules. `nestedFlow` will inherit from `nestedSink`
except the `map` operator which has again an explicitly provided attribute overriding the inherited one.

![compose_attributes.png](../images/compose_attributes.png)

This diagram illustrates the inheritance process for the example code (representing the materializer default attributes
as the color *red*, the attributes set on `nestedSink` as *blue* and the attributes set on `nestedFlow` as *green*).
