/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Balance, Flow, GraphDSL, Merge }
import akka.testkit.AkkaSpec

import scala.annotation.nowarn

@nowarn("msg=never used") // sample snippets
class FlowParallelismDocSpec extends AkkaSpec {

  import GraphDSL.Implicits._

  case class ScoopOfBatter()
  case class HalfCookedPancake()
  case class Pancake()

  //format: OFF
  //#pipelining
  // Takes a scoop of batter and creates a pancake with one side cooked
  val fryingPan1: Flow[ScoopOfBatter, HalfCookedPancake, NotUsed] =
    Flow[ScoopOfBatter].map { batter =>
      HalfCookedPancake()
    }

  // Finishes a half-cooked pancake
  val fryingPan2: Flow[HalfCookedPancake, Pancake, NotUsed] =
    Flow[HalfCookedPancake].map { halfCooked =>
      Pancake()
    }
  //#pipelining
  //format: ON

  "Demonstrate pipelining" in {
    //#pipelining

    // With the two frying pans we can fully cook pancakes
    val pancakeChef: Flow[ScoopOfBatter, Pancake, NotUsed] =
      Flow[ScoopOfBatter].via(fryingPan1.async).via(fryingPan2.async)
    //#pipelining
  }

  "Demonstrate parallel processing" in {
    //#parallelism
    val fryingPan: Flow[ScoopOfBatter, Pancake, NotUsed] =
      Flow[ScoopOfBatter].map { batter =>
        Pancake()
      }

    val pancakeChef: Flow[ScoopOfBatter, Pancake, NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder =>
      val dispatchBatter = builder.add(Balance[ScoopOfBatter](2))
      val mergePancakes = builder.add(Merge[Pancake](2))

      // Using two frying pans in parallel, both fully cooking a pancake from the batter.
      // We always put the next scoop of batter to the first frying pan that becomes available.
      dispatchBatter.out(0) ~> fryingPan.async ~> mergePancakes.in(0)
      // Notice that we used the "fryingPan" flow without importing it via builder.add().
      // Flows used this way are auto-imported, which in this case means that the two
      // uses of "fryingPan" mean actually different stages in the graph.
      dispatchBatter.out(1) ~> fryingPan.async ~> mergePancakes.in(1)

      FlowShape(dispatchBatter.in, mergePancakes.out)
    })

    //#parallelism
  }

  "Demonstrate parallelized pipelines" in {
    //#parallel-pipeline
    val pancakeChef: Flow[ScoopOfBatter, Pancake, NotUsed] =
      Flow.fromGraph(GraphDSL.create() { implicit builder =>
        val dispatchBatter = builder.add(Balance[ScoopOfBatter](2))
        val mergePancakes = builder.add(Merge[Pancake](2))

        // Using two pipelines, having two frying pans each, in total using
        // four frying pans
        dispatchBatter.out(0) ~> fryingPan1.async ~> fryingPan2.async ~> mergePancakes.in(0)
        dispatchBatter.out(1) ~> fryingPan1.async ~> fryingPan2.async ~> mergePancakes.in(1)

        FlowShape(dispatchBatter.in, mergePancakes.out)
      })
    //#parallel-pipeline
  }

  "Demonstrate pipelined parallel processing" in {
    //#pipelined-parallel
    val pancakeChefs1: Flow[ScoopOfBatter, HalfCookedPancake, NotUsed] =
      Flow.fromGraph(GraphDSL.create() { implicit builder =>
        val dispatchBatter = builder.add(Balance[ScoopOfBatter](2))
        val mergeHalfPancakes = builder.add(Merge[HalfCookedPancake](2))

        // Two chefs work with one frying pan for each, half-frying the pancakes then putting
        // them into a common pool
        dispatchBatter.out(0) ~> fryingPan1.async ~> mergeHalfPancakes.in(0)
        dispatchBatter.out(1) ~> fryingPan1.async ~> mergeHalfPancakes.in(1)

        FlowShape(dispatchBatter.in, mergeHalfPancakes.out)
      })

    val pancakeChefs2: Flow[HalfCookedPancake, Pancake, NotUsed] =
      Flow.fromGraph(GraphDSL.create() { implicit builder =>
        val dispatchHalfPancakes = builder.add(Balance[HalfCookedPancake](2))
        val mergePancakes = builder.add(Merge[Pancake](2))

        // Two chefs work with one frying pan for each, finishing the pancakes then putting
        // them into a common pool
        dispatchHalfPancakes.out(0) ~> fryingPan2.async ~> mergePancakes.in(0)
        dispatchHalfPancakes.out(1) ~> fryingPan2.async ~> mergePancakes.in(1)

        FlowShape(dispatchHalfPancakes.in, mergePancakes.out)
      })

    val kitchen: Flow[ScoopOfBatter, Pancake, NotUsed] = pancakeChefs1.via(pancakeChefs2)
    //#pipelined-parallel

  }

}
