# Futures patterns

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Akka offers tiny helpers for use with @scala[@scaladoc[Future](scala.concurrent.Future)s]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)]. These are part of Akka's core module:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor_$scala.binary.version$"
  version=AkkaVersion
}

## After

@scala[`akka.pattern.after`]@java[@javadoc[akka.pattern.Patterns.after](akka.pattern.Patterns#after)] makes it easy to complete a @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)] with a value or exception after a timeout.

Scala
:  @@snip [FutureDocSpec.scala](/akka-docs/src/test/scala/docs/future/FutureDocSpec.scala) { #after }

Java
:   @@snip [FutureDocTest.java](/akka-docs/src/test/java/jdocs/future/FutureDocTest.java) { #imports #after }

## Retry

@scala[`akka.pattern.retry`]@java[@javadoc[akka.pattern.Patterns.retry](akka.pattern.Patterns#retry)] will retry a @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)] some number of times with a delay between each attempt.

Scala
:   @@snip [FutureDocSpec.scala](/akka-docs/src/test/scala/docs/future/FutureDocSpec.scala) { #retry }

Java
:   @@snip [FutureDocTest.java](/akka-docs/src/test/java/jdocs/future/FutureDocTest.java) { #imports #retry }
