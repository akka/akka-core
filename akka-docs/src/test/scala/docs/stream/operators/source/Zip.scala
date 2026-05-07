/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.source

import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Source

object Zip {

  implicit val system: ActorSystem[_] = ???

  def zipN(): Unit = {
    // #zipN-simple
    val chars = Source("a" :: "b" :: "c" :: "e" :: "f" :: Nil)
    val numbers = Source(1 :: 2 :: 3 :: 4 :: 5 :: 6 :: Nil)
    val colors = Source("red" :: "green" :: "blue" :: "yellow" :: "purple" :: Nil)

    Source.zipN(chars :: numbers :: colors :: Nil).runForeach(println)
    // prints:
    // Vector(a, 1, red)
    // Vector(b, 2, green)
    // Vector(c, 3, blue)
    // Vector(e, 4, yellow)
    // Vector(f, 5, purple)

    // #zipN-simple
  }

  def zipNWith(): Unit = {
    // #zipWithN-simple
    val numbers = Source(1 :: 2 :: 3 :: 4 :: 5 :: 6 :: Nil)
    val otherNumbers = Source(5 :: 2 :: 1 :: 4 :: 10 :: 4 :: Nil)
    val andSomeOtherNumbers = Source(3 :: 7 :: 2 :: 1 :: 1 :: Nil)

    Source
      .zipWithN((seq: Seq[Int]) => seq.max)(numbers :: otherNumbers :: andSomeOtherNumbers :: Nil)
      .runForeach(println)
    // prints:
    // 5
    // 7
    // 3
    // 4
    // 10

    // #zipWithN-simple
  }

  def zipAll(): Unit = {
    // #zipAll-simple
    val numbers = Source(1 :: 2 :: 3 :: 4 :: Nil)
    val letters = Source("a" :: "b" :: "c" :: Nil)

    numbers.zipAll(letters, -1, "default").runForeach(println)
    // prints:
    // (1,a)
    // (2,b)
    // (3,c)
    // (4,default)
    // #zipAll-simple
  }

  def zipLatest(): Unit = {
    // #zipLatest-example
    val numbers = Source(1 :: 2 :: Nil)
    val letters = Source("a" :: "b" :: Nil)

    numbers.zipLatest(letters).runForeach(println)
    // this will print
    // (1,a)
    // (2,a)
    // (2,b)
    // NOTE : The output is not always deterministic and also depends on order of elements flowing from the streams.
    // Sometimes the output could also be :
    // (1, a)
    // (1, b)
    // (2, b)
    // #zipLatest-example
  }
}
