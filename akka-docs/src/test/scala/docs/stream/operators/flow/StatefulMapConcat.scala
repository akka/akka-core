/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.flow

import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source

class StatefulMapConcat {

  implicit val system: ActorSystem = ???

  def zipWithIndex(): Unit = {
    // #zip-with-index
    val letterAndIndex = Source("a" :: "b" :: "c" :: "d" :: Nil).statefulMapConcat { () =>
      var index = 0L

      // we return the function that will be invoked for each element
      { element =>
        val zipped = (element, index)
        index += 1
        // we return an iterable with the single element
        zipped :: Nil
      }
    }

    letterAndIndex.runForeach(println)
    // prints
    // (a,0)
    // (b,1)
    // (c,2)
    // (d,3)
    // #zip-with-index
  }

  def denylist(): Unit = {
    // #denylist
    val fruitsAndDeniedCommands = Source(
      "banana" :: "pear" :: "orange" :: "deny:banana" :: "banana" :: "pear" :: "banana" :: Nil)

    val denyFilterFlow = Flow[String].statefulMapConcat { () =>
      var denyList = Set.empty[String]

      { element =>
        if (element.startsWith("deny:")) {
          denyList += element.drop("deny:".size)
          Nil // no element downstream when adding a deny listed keyword
        } else if (denyList(element)) {
          Nil // no element downstream if element is deny listed
        } else {
          element :: Nil
        }
      }
    }

    fruitsAndDeniedCommands.via(denyFilterFlow).runForeach(println)
    // prints
    // banana
    // pear
    // orange
    // pear
    // #denylist
  }

  def reactOnEnd(): Unit = {
    // #bs-last
    val words = Source("baboon" :: "crocodile" :: "bat" :: "flamingo" :: "hedgehog" :: "beaver" :: Nil)

    val bWordsLast = Flow[String].concat(Source.single("-end-")).statefulMapConcat { () =>
      var stashedBWords: List[String] = Nil

      { element =>
        if (element.startsWith("b")) {
          // prepend to stash and emit no element
          stashedBWords = element :: stashedBWords
          Nil
        } else if (element.equals("-end-")) {
          // return in the stashed words in the order they got stashed
          stashedBWords.reverse
        } else {
          // emit the element as is
          element :: Nil
        }
      }
    }

    words.via(bWordsLast).runForeach(println)
    // prints
    // crocodile
    // flamingo
    // hedgehog
    // baboon
    // bat
    // beaver
    // #bs-last
  }

}
