/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.flow
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

object StatefulMap {

  implicit val actorSystem: ActorSystem = ???

  def indexed(): Unit = {
    //#zipWithIndex
    Source(List("A", "B", "C", "D"))
      .statefulMap(() => 0L)((index, elem) => (index + 1, (elem, index)), _ => None)
      .runForeach(println)
    //prints
    //(A,0)
    //(B,1)
    //(C,2)
    //(D,3)
    //#zipWithIndex
  }

  def bufferUntilChanged(): Unit = {
    //#bufferUntilChanged
    Source("A" :: "B" :: "B" :: "C" :: "C" :: "C" :: "D" :: Nil)
      .statefulMap(() => List.empty[String])(
        (buffer, element) =>
          buffer match {
            case head :: _ if head != element => (element :: Nil, buffer)
            case _                            => (element :: buffer, Nil)
          },
        buffer => Some(buffer))
      .filter(_.nonEmpty)
      .runForeach(println)
    //prints
    //List(A)
    //List(B, B)
    //List(C, C, C)
    //List(D)
    //#bufferUntilChanged
  }

  def distinctUntilChanged(): Unit = {
    //#distinctUntilChanged
    Source("A" :: "B" :: "B" :: "C" :: "C" :: "C" :: "D" :: Nil)
      .statefulMap(() => Option.empty[String])(
        (lastElement, elem) =>
          lastElement match {
            case Some(head) if head == elem => (Some(elem), None)
            case _                          => (Some(elem), Some(elem))
          },
        _ => None)
      .collect { case Some(elem) => elem }
      .runForeach(println)
    //prints
    //A
    //B
    //C
    //D
    //#distinctUntilChanged
  }

  def statefulMapConcatLike(): Unit = {
    //#statefulMapConcatLike
    Source(1 to 10)
      .statefulMap(() => List.empty[Int])(
        (state, elem) => {
          //grouped 3 elements into a list
          val newState = elem :: state
          if (newState.size == 3)
            (Nil, newState.reverse)
          else
            (newState, Nil)
        },
        state => Some(state.reverse))
      .mapConcat(identity)
      .runForeach(println)
    //prints
    //1
    //2
    //3
    //4
    //5
    //6
    //7
    //8
    //9
    //10
    //#statefulMapConcatLike
  }
}
