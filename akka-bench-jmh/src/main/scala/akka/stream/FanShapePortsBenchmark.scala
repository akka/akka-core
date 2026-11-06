/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class FanShapePortsBenchmark {
  private val in0: Inlet[Int] = Inlet[Int]("in0")
  private val in1: Inlet[Int] = Inlet[Int]("in1")
  private val in2: Inlet[Int] = Inlet[Int]("in2")
  private val in3: Inlet[Int] = Inlet[Int]("in3")
  private val in4: Inlet[Int] = Inlet[Int]("in4")
  private val in5: Inlet[Int] = Inlet[Int]("in5")
  private val in6: Inlet[Int] = Inlet[Int]("in6")
  private val in7: Inlet[Int] = Inlet[Int]("in7")
  private val in8: Inlet[Int] = Inlet[Int]("in8")
  private val in9: Inlet[Int] = Inlet[Int]("in9")
  private val in10: Inlet[Int] = Inlet[Int]("in10")
  private val in11: Inlet[Int] = Inlet[Int]("in11")
  private val in12: Inlet[Int] = Inlet[Int]("in12")
  private val in13: Inlet[Int] = Inlet[Int]("in13")
  private val in14: Inlet[Int] = Inlet[Int]("in14")
  private val in15: Inlet[Int] = Inlet[Int]("in15")
  private val in16: Inlet[Int] = Inlet[Int]("in16")
  private val in17: Inlet[Int] = Inlet[Int]("in17")
  private val in18: Inlet[Int] = Inlet[Int]("in18")
  private val in19: Inlet[Int] = Inlet[Int]("in19")
  private val in20: Inlet[Int] = Inlet[Int]("in20")
  private val in21: Inlet[Int] = Inlet[Int]("in21")
  private val out: Outlet[Int] = Outlet[Int]("out")

  @Benchmark
  def cons_2(): List[Inlet[_]] =
    in0 :: in1 :: Nil

  @Benchmark
  def ascribed_2(): List[Inlet[_]] =
    in0 :: in1 :: (Nil: List[Inlet[_]])

  @Benchmark
  def apply_2(): List[Inlet[_]] =
    List[Inlet[_]](in0, in1)

  @Benchmark
  def constructor_2(): FanInShape[Int] =
    new FanInShape2(in0, in1, out)

  @Benchmark
  def cons_7(): List[Inlet[_]] =
    in0 :: in1 :: in2 :: in3 :: in4 :: in5 :: in6 :: Nil

  @Benchmark
  def ascribed_7(): List[Inlet[_]] =
    in0 :: in1 :: in2 :: in3 :: in4 :: in5 :: in6 :: (Nil: List[Inlet[_]])

  @Benchmark
  def apply_7(): List[Inlet[_]] =
    List[Inlet[_]](in0, in1, in2, in3, in4, in5, in6)

  @Benchmark
  def constructor_7(): FanInShape[Int] =
    new FanInShape7(in0, in1, in2, in3, in4, in5, in6, out)

  @Benchmark
  def cons_8(): List[Inlet[_]] =
    in0 :: in1 :: in2 :: in3 :: in4 :: in5 :: in6 :: in7 :: Nil

  @Benchmark
  def ascribed_8(): List[Inlet[_]] =
    in0 :: in1 :: in2 :: in3 :: in4 :: in5 :: in6 :: in7 :: (Nil: List[Inlet[_]])

  @Benchmark
  def apply_8(): List[Inlet[_]] =
    List[Inlet[_]](in0, in1, in2, in3, in4, in5, in6, in7)

  @Benchmark
  def constructor_8(): FanInShape[Int] =
    new FanInShape8(in0, in1, in2, in3, in4, in5, in6, in7, out)

  @Benchmark
  def cons_22(): List[Inlet[_]] =
    in0 :: in1 :: in2 :: in3 :: in4 :: in5 :: in6 :: in7 :: in8 :: in9 :: in10 ::
    in11 :: in12 :: in13 :: in14 :: in15 :: in16 :: in17 :: in18 :: in19 :: in20 :: in21 :: Nil

  @Benchmark
  def ascribed_22(): List[Inlet[_]] =
    in0 :: in1 :: in2 :: in3 :: in4 :: in5 :: in6 :: in7 :: in8 :: in9 :: in10 ::
    in11 :: in12 :: in13 :: in14 :: in15 :: in16 :: in17 :: in18 :: in19 :: in20 :: in21 :: (Nil: List[Inlet[_]])

  @Benchmark
  def apply_22(): List[Inlet[_]] =
    List[Inlet[_]](
      in0,
      in1,
      in2,
      in3,
      in4,
      in5,
      in6,
      in7,
      in8,
      in9,
      in10,
      in11,
      in12,
      in13,
      in14,
      in15,
      in16,
      in17,
      in18,
      in19,
      in20,
      in21)

  @Benchmark
  def constructor_22(): FanInShape[Int] =
    new FanInShape22(
      in0,
      in1,
      in2,
      in3,
      in4,
      in5,
      in6,
      in7,
      in8,
      in9,
      in10,
      in11,
      in12,
      in13,
      in14,
      in15,
      in16,
      in17,
      in18,
      in19,
      in20,
      in21,
      out)

}
