# Shopping cart example

The provided CRDT data structures can be used as the root state of a replicated `EventSourcedBehavior` but they can
also be nested inside another data structure. This requires a bit more careful thinking about the eventual consistency.
 
In this sample we model a shopping cart as a map of product ids and the number of that product added or removed in the
shopping cart. By using the @apidoc[Counter] CRDT and persisting its `Update` in our events we can be sure that an
add or remove of items in any replica will eventually lead to all replicas ending up with the same number of
each product. 
 
Scala
:   @@snip [ShoppingCartExample](/akka-persistence-typed-tests/src/test/scala/docs/akka/persistence/typed/ReplicatedShoppingCartExampleSpec.scala) { #shopping-cart }

Java
:   @@snip [ShoppingCartExample](/akka-persistence-typed-tests/src/test/java/jdocs/akka/persistence/typed/ReplicatedShoppingCartExample.java) { #shopping-cart }

With this model we cannot have a `ClearCart` command as that could give different states in different replicas.
It is quite easy to imagine such a scenario: commands arriving in the order `ClearCart`, `AddItem('a', 5)` in one
replica and the order `AddItem('a', 5), ClearCart` in another.
 
To clear a cart a client would instead have to remove as many items of each product as it sees in the cart at the time
of removal.
