/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import akka.stream.impl.JsonObjectParser
import akka.util.ByteString

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class JsonFramingBenchmark {

  val json =
    ByteString("""{"fname":"Frank","name":"Smith","age":42,"id":1337,"boardMember":false}""")

  val json5 =
    ByteString("""|{"fname":"Frank","name":"Smith","age":42,"id":1337,"boardMember":false},
         |{"fname":"Bob","name":"Smith","age":42,"id":1337,"boardMember":false},
         |{"fname":"Bob","name":"Smith","age":42,"id":1337,"boardMember":false},
         |{"fname":"Bob","name":"Smith","age":42,"id":1337,"boardMember":false},
         |{"fname":"Hank","name":"Smith","age":42,"id":1337,"boardMember":false}""".stripMargin)

  val jsonLong =
    ByteString(
      s"""{"fname":"Frank","name":"Smith","age":42,"id":1337,"boardMember":false,"description":"${"a" * 1000000}"}""")

  val bracket = new JsonObjectParser

  @Benchmark
  def counting_1: ByteString = {
    bracket.offer(json)
    bracket.poll().get
  }

  @Benchmark
  @OperationsPerInvocation(5)
  def counting_offer_5: ByteString = {
    bracket.offer(json5)
    bracket.poll().get
    bracket.poll().get
    bracket.poll().get
    bracket.poll().get
    bracket.poll().get
  }

  @Benchmark
  def counting_long_document: ByteString = {
    bracket.offer(jsonLong)
    bracket.poll().get
  }

}
