# Circuit Breaker

## Why are they used?

A circuit breaker is used to provide stability and prevent cascading failures in distributed
systems.  These should be used in conjunction with judicious timeouts at the interfaces between
remote systems to prevent the failure of a single component from bringing down all components.

As an example, we have a web application interacting with a remote third party web service.  
Let's say the third party has oversold their capacity and their database melts down under load.  
Assume that the database fails in such a way that it takes a very long time to hand back an
error to the third party web service.  This in turn makes calls fail after a long period of 
time.  Back to our web application, the users have noticed that their form submissions take
much longer seeming to hang.  Well the users do what they know to do which is use the refresh
button, adding more requests to their already running requests.  This eventually causes the 
failure of the web application due to resource exhaustion.  This will affect all users, even
those who are not using functionality dependent on this third party web service.

Introducing circuit breakers on the web service call would cause the requests to begin to 
fail-fast, letting the user know that something is wrong and that they need not refresh 
their request.  This also confines the failure behavior to only those users that are using
functionality dependent on the third party, other users are no longer affected as there is no
resource exhaustion.  Circuit breakers can also allow savvy developers to mark portions of
the site that use the functionality unavailable, or perhaps show some cached content as 
appropriate while the breaker is open.

Like any other pattern, circuit breakers have their own challenges: circuit breakers can misinterpret a partial failure as total system failure and inadvertently bring down the entire system. In particular, sharded systems and cell-based architectures are vulnerable to this issue. For example, let’s say only one of your database shards is overloaded and other shards are working normally. Usually in such circumstances, the circuit breaker either assumes the entire database (i.e. all shards) is overloaded and trips which negatively impacts the normal shards, or the circuit breaker assumes the required threshold hasn’t been exceeded and doesn’t do anything to mitigate overloading of the problematic shard. In either case, the result is not optimal. A workaround is that the server indicates to the client exactly which specific part is overloaded and the client uses a corresponding mini circuit breaker. However, this workaround can be complex and expensive.

The Akka library provides an implementation of a circuit breaker called 
@apidoc[CircuitBreaker] which has the behavior described below.

## What do they do?

* During normal operation, a circuit breaker is in the *Closed* state:

    - Exceptions or calls exceeding the configured *callTimeout* increment a failure counter
    - Successes reset the failure count to zero
    - When the failure counter reaches a *maxFailures* count, the breaker is tripped into *Open* state
  
* While in *Open* state:

    - All calls fail-fast with a @apidoc[CircuitBreakerOpenException]
    - After the configured *resetTimeout*, the circuit breaker enters a *Half-Open* state
  
* In *Half-Open* state:

    - The first call attempted is allowed through without failing fast
    - All other calls fail-fast with an exception just as in *Open* state
    - If the first call succeeds, the breaker is reset back to *Closed* state and the *resetTimeout* is reset
    - If the first call fails, the breaker is tripped again into the *Open* state (as for exponential backoff circuit breaker, the *resetTimeout* is multiplied by the exponential backoff factor)
  
* State transition listeners:

    - Callbacks can be provided for every state entry via @apidoc[onOpen](CircuitBreaker) {scala="#onOpen(callback:=%3EUnit):akka.pattern.CircuitBreaker" java="#addOnOpenListener(java.lang.Runnable)"}, @apidoc[onClose](CircuitBreaker) {scala="#onClose(callback:=%3EUnit):akka.pattern.CircuitBreaker" java="#addOnCloseListener(java.lang.Runnable)"}, and @apidoc[onHalfOpen](CircuitBreaker) {scala="#onHalfOpen(callback:=%3EUnit):akka.pattern.CircuitBreaker" java="#addOnHalfOpenListener(java.lang.Runnable)"}
    - These are executed in the @scaladoc[ExecutionContext](scala.concurrent.ExecutionContext) provided.
  
* Calls result listeners:

    - Callbacks can be used eg. to collect statistics about all invocations or to react on specific call results like success, failures or timeouts.
    - Supported callbacks are: @apidoc[onCallSuccess](CircuitBreaker) {scala="#onCallSuccess(callback:Long=%3EUnit):akka.pattern.CircuitBreaker" java="#addOnCallSuccessListener(java.util.function.Consumer)"}, @apidoc[onCallFailure](CircuitBreaker) {scala="#onCallFailure(callback:Long=%3EUnit):akka.pattern.CircuitBreaker" java="#addOnCallFailureListener(java.util.function.Consumer)"}, @apidoc[onCallTimeout](CircuitBreaker) {scala="#onCallTimeout(callback:Long=%3EUnit):akka.pattern.CircuitBreaker" java="#addOnCallTimeoutListener(java.util.function.Consumer)"}, @apidoc[onCallBreakerOpen](CircuitBreaker) {scala="#onCallBreakerOpen(callback:=%3EUnit):akka.pattern.CircuitBreaker" java="#addOnCallBreakerOpenListener(java.lang.Runnable)"}.
    - These are executed in the @scaladoc[ExecutionContext](scala.concurrent.ExecutionContext) provided.
   

![circuit-breaker-states.png](../images/circuit-breaker-states.png)

## Examples

### Initialization

Here's how a named @apidoc[CircuitBreaker] is configured with the name `data-access`:

* 5 maximum failures
* a call timeout of 10 seconds
* a reset timeout of 1 minute

@@snip [application.conf](/akka-docs/src/test/scala/docs/circuitbreaker/CircuitBreakerDocSpec.scala) { #config }

The circuit breaker is created on first access with the same name, subsequent lookups will return the same circuit breaker
instance. Looking up the circuit breaker and using it looks like this:

Scala
:  @@snip [CircuitBreakerDocSpec.scala](/akka-docs/src/test/scala/docs/circuitbreaker/CircuitBreakerDocSpec.scala) { #circuit-breaker-initialization }

Java
:  @@snip [DangerousJavaActor.java](/akka-docs/src/test/java/jdocs/circuitbreaker/CircuitBreakerDocTest.java) { #circuit-breaker-initialization }

### Future & Synchronous based API

Once a circuit breaker actor has been initialized, interacting with that actor is done by either using the Future based API or the synchronous API. Both of these APIs are considered `Call Protection` because whether synchronously or asynchronously, the purpose of the circuit breaker is to protect your system from cascading failures while making a call to another service. 

In the future based API, we use the @scala[@scaladoc[withCircuitBreaker](akka.pattern.CircuitBreaker#withCircuitBreaker[T](body:=%3Escala.concurrent.Future[T]):scala.concurrent.Future[T])]@java[@javadoc[callWithCircuitBreakerCS](akka.pattern.CircuitBreaker#callWithCircuitBreakerCS(java.util.concurrent.Callable))] which takes an asynchronous method (some method wrapped in a @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionState](java.util.concurrent.CompletionStage)]), for instance a call to retrieve data from a service, and we pipe the result back to the sender. If for some reason the service in this example isn't responding, or there is another issue, the circuit breaker will open and stop trying to hit the service again and again until the timeout is reached.

Scala
: @@snip [CircuitBreakerDocSpec.scala](/akka-docs/src/test/scala/docs/circuitbreaker/CircuitBreakerDocSpec.scala) { #circuit-breaker-usage }

Java
:  @@snip [CircuitBreakerDocTest.java](/akka-docs/src/test/java/jdocs/circuitbreaker/CircuitBreakerDocTest.java) { #circuit-breaker-usage }

The Synchronous API would also wrap your call with the circuit breaker logic, however, it uses the @scala[@scaladoc[withSyncCircuitBreaker](akka.pattern.CircuitBreaker#withSyncCircuitBreaker[T](body:=%3ET):T)]@java[@javadoc[callWithSyncCircuitBreaker](akka.pattern.CircuitBreaker#callWithSyncCircuitBreaker(java.util.concurrent.Callable))] and receives a method that is not wrapped in a @scala[`Future`]@java[`CompletionState`].

The `CircuitBreaker` will execute all callbacks on the default system dispatcher.

### Control failure count explicitly

By default, the circuit breaker treats @javadoc[Exception](java.lang.Exception) as failure in synchronized API, or failed @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionState](java.util.concurrent.CompletionStage)] as failure in future based API.
On failure, the failure count will increment. If the failure count reaches the *maxFailures*, the circuit breaker will be opened.
However, some applications may require certain exceptions to not increase the failure count.
In other cases one may want to increase the failure count even if the call succeeded.
Akka circuit breaker provides a way to achieve such use cases: @scala[@scaladoc[withCircuitBreaker](akka.pattern.CircuitBreaker#withCircuitBreaker[T](body:=%3Escala.concurrent.Future[T],defineFailureFn:scala.util.Try[T]=%3EBoolean):scala.concurrent.Future[T]) and @scaladoc[withSyncCircuitBreaker](akka.pattern.CircuitBreaker#withSyncCircuitBreaker[T](body:=%3ET,defineFailureFn:scala.util.Try[T]=%3EBoolean):T)]@java[@javadoc[callWithCircuitBreaker](akka.pattern.CircuitBreaker#callWithCircuitBreaker(java.util.concurrent.Callable,java.util.function.BiFunction)), @javadoc[callWithSyncCircuitBreaker](akka.pattern.CircuitBreaker#callWithSyncCircuitBreaker(java.util.concurrent.Callable,java.util.function.BiFunction)) and @javadoc[callWithCircuitBreakerCS](akka.pattern.CircuitBreaker#callWithCircuitBreakerCS(java.util.concurrent.Callable,java.util.function.BiFunction))].

All methods above accept an argument `defineFailureFn`

Type of `defineFailureFn`: @scala[@scaladoc[Try[T]](scala.util.Try) => @scaladoc[Boolean](scala.Boolean)]@java[@javadoc[BiFunction](java.util.function.BiFunction)[@javadoc[Optional[T]](java.util.Optional), @javadoc[Optional](java.util.Optional)[@javadoc[Throwable](java.lang.Throwable)], @javadoc[Boolean](java.lang.Boolean)]]

@scala[This is a function which takes in a @scaladoc[Try[T]](scala.util.Try) and returns a @scaladoc[Boolean](scala.Boolean). The @scaladoc[Try[T]](scala.util.Try) correspond to the @scaladoc[Future[T]](scala.concurrent.Future) of the protected call.]
@java[The response of a protected call is modelled using @javadoc[Optional[T]](java.util.Optional) for a successful return value and @javadoc[Optional](java.util.Optional)[@javadoc[Throwable](java.lang.Throwable)] for exceptions.] This function should return `true` if the result of a call should increase the failure count, or `false` to not affect the count.

Scala
:  @@snip [CircuitBreakerDocSpec.scala](/akka-docs/src/test/scala/docs/circuitbreaker/CircuitBreakerDocSpec.scala) { #even-no-as-failure }

Java
:  @@snip [CircuitBreakerDocTest.java](/akka-docs/src/test/java/jdocs/circuitbreaker/CircuitBreakerDocTest.java) { #even-no-as-failure }

### Low level API

Instead of looking up a configured circuit breaker by name, it is also possible to construct it in the source code:

Scala
:  @@snip [CircuitBreakerDocSpec.scala](/akka-docs/src/test/scala/docs/circuitbreaker/CircuitBreakerDocSpec.scala) { #manual-construction }

Java
:  @@snip [CircuitBreakerDocTest.java](/akka-docs/src/test/java/jdocs/circuitbreaker/CircuitBreakerDocTest.java) { #manual-construction }

This also allows for creating the circuit breaker with a specific execution context to run its callbacks on.

The low-level API allows you to describe the behavior of the @apidoc[CircuitBreaker](CircuitBreaker) in detail, including deciding what to return to the calling @apidoc[Actor](Actor) in case of success or failure. This is especially useful when expecting the remote call to send a reply.
@apidoc[CircuitBreaker](CircuitBreaker) doesn't support `Tell Protection` (protecting against calls that expect a reply) natively at the moment.
Thus, you need to use the low-level power-user APIs, @apidoc[succeed](CircuitBreaker) {scala="#succeed():Unit" java="#succeed()"}  and  @apidoc[fail](CircuitBreaker) {scala="#fail():Unit" java="#fail()"} methods, as well as @apidoc[isClosed](CircuitBreaker) {scala="#isClosed:Boolean" java="#isClosed()"}, @apidoc[isOpen](CircuitBreaker) {scala="#isOpen:Boolean" java="#isOpen()"}, @apidoc[isHalfOpen](CircuitBreaker) {scala="#isHalfOpen:Boolean" java="#isHalfOpen()"} to implement it.

As can be seen in the examples below, a `Tell Protection` pattern could be implemented by using the @apidoc[succeed](CircuitBreaker) {scala="#succeed():Unit" java="#succeed()"}  and  @apidoc[fail](CircuitBreaker) {scala="#fail():Unit" java="#fail()"} methods, which would count towards the @apidoc[CircuitBreaker](CircuitBreaker) counts. 
In the example, a call is made to the remote service if the breaker is closed or half open. 
Once a response is received, the @apidoc[succeed](CircuitBreaker) {scala="#succeed():Unit" java="#succeed()"} method is invoked, which tells the @apidoc[CircuitBreaker](CircuitBreaker) to keep the breaker closed. 
On the other hand, if an error or timeout is received we trigger a @apidoc[fail](CircuitBreaker) {scala="#fail():Unit" java="#fail()"}, and the breaker accrues this failure towards its count for opening the breaker.

Scala
:  @@snip [CircuitBreakerDocSpec.scala](/akka-docs/src/test/scala/docs/circuitbreaker/CircuitBreakerDocSpec.scala) { #circuit-breaker-tell-pattern }

Java
:  @@snip [CircuitBreakerDocTest.java](/akka-docs/src/test/java/jdocs/circuitbreaker/CircuitBreakerDocTest.java) { #circuit-breaker-tell-pattern }

@@@ note

This example always makes remote calls when the state is *HalfOpen*. Using the power-user APIs, it is your responsibility to judge when to make remote calls in *HalfOpen*.

@@@
