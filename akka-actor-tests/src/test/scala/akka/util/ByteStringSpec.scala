/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import java.lang.Double.doubleToRawLongBits
import java.lang.Float.floatToRawIntBits
import java.nio.{ ByteBuffer, ByteOrder }
import java.nio.ByteOrder.{ BIG_ENDIAN, LITTLE_ENDIAN }

import scala.annotation.nowarn
import scala.collection.mutable.Builder

import org.apache.commons.codec.binary.Hex.encodeHex
import org.scalacheck.{ Arbitrary, Gen }
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.Checkers

import akka.util.ByteString.{ ByteString1, ByteString1C, ByteStrings }

@nowarn("cat=lint-infer-any")
class ByteStringSpec extends AnyWordSpec with Matchers with Checkers {

  implicit val betterGeneratorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration().copy(minSuccessful = 1000)

  def genSimpleByteString(min: Int, max: Int) =
    for {
      n <- Gen.choose(min, max)
      b <- Gen.containerOfN[Array, Byte](n, arbitrary[Byte])
      from <- Gen.choose(0, b.length)
      until <- Gen.choose(from, from max b.length)
    } yield ByteString(b).slice(from, until)

  implicit val arbitraryByteString: Arbitrary[ByteString] = Arbitrary {
    Gen.sized { s =>
      for {
        chunks <- Gen.choose(0, s)
        bytes <- Gen.listOfN(chunks, genSimpleByteString(1, 1 max (s / (chunks max 1))))
      } yield bytes.foldLeft(ByteString.empty)(_ ++ _)
    }
  }

  type ByteStringSlice = (ByteString, Int, Int)

  implicit val arbitraryByteStringSlice: Arbitrary[ByteStringSlice] = Arbitrary {
    for {
      xs <- arbitraryByteString.arbitrary
      from <- Gen.choose(0, 0 max (xs.length - 1))
      until <- {
        require(from <= xs.length)
        Gen.choose(from, xs.length)
      }
    } yield (xs, from, until)
  }

  case class ByteStringGrouped(bs: ByteString, size: Int)

  implicit val arbitraryByteStringGrouped: Arbitrary[ByteStringGrouped] = Arbitrary {
    for {
      xs <- arbitraryByteString.arbitrary
      size <- Gen.choose(1, 1 max xs.length)
    } yield ByteStringGrouped(xs, size)
  }

  type ArraySlice[A] = (Array[A], Int, Int)

  def arbSlice[A](arbArray: Arbitrary[Array[A]]): Arbitrary[ArraySlice[A]] = Arbitrary {
    for {
      xs <- arbArray.arbitrary
      from <- Gen.choose(0, xs.length)
      until <- Gen.choose(from, xs.length)
    } yield (xs, from, until)
  }

  def serialize(obj: AnyRef): Array[Byte] = {
    val os = new ByteArrayOutputStream
    val bos = new ObjectOutputStream(os)
    bos.writeObject(obj)
    os.toByteArray
  }

  def deserialize(bytes: Array[Byte]): AnyRef = {
    val is = new ObjectInputStream(new ByteArrayInputStream(bytes))

    is.readObject
  }

  def testSer(obj: AnyRef) = {
    deserialize(serialize(obj)) == obj
  }

  def hexFromSer(obj: AnyRef) = {
    val os = new ByteArrayOutputStream
    val bos = new ObjectOutputStream(os)
    bos.writeObject(obj)
    String.valueOf(encodeHex(os.toByteArray))
  }

  val arbitraryByteArray: Arbitrary[Array[Byte]] = Arbitrary {
    Gen.sized { n =>
      Gen.containerOfN[Array, Byte](n, arbitrary[Byte])
    }
  }
  implicit val arbitraryByteArraySlice: Arbitrary[ArraySlice[Byte]] = arbSlice(arbitraryByteArray)
  val arbitraryShortArray: Arbitrary[Array[Short]] = Arbitrary {
    Gen.sized { n =>
      Gen.containerOfN[Array, Short](n, arbitrary[Short])
    }
  }
  implicit val arbitraryShortArraySlice: Arbitrary[ArraySlice[Short]] = arbSlice(arbitraryShortArray)
  val arbitraryIntArray: Arbitrary[Array[Int]] = Arbitrary {
    Gen.sized { n =>
      Gen.containerOfN[Array, Int](n, arbitrary[Int])
    }
  }
  implicit val arbitraryIntArraySlice: Arbitrary[ArraySlice[Int]] = arbSlice(arbitraryIntArray)
  val arbitraryLongArray: Arbitrary[Array[Long]] = Arbitrary {
    Gen.sized { n =>
      Gen.containerOfN[Array, Long](n, arbitrary[Long])
    }
  }
  implicit val arbitraryLongArraySlice: Arbitrary[ArraySlice[Long]] = arbSlice(arbitraryLongArray)
  val arbitraryFloatArray: Arbitrary[Array[Float]] = Arbitrary {
    Gen.sized { n =>
      Gen.containerOfN[Array, Float](n, arbitrary[Float])
    }
  }
  implicit val arbitraryFloatArraySlice: Arbitrary[ArraySlice[Float]] = arbSlice(arbitraryFloatArray)
  val arbitraryDoubleArray: Arbitrary[Array[Double]] = Arbitrary {
    Gen.sized { n =>
      Gen.containerOfN[Array, Double](n, arbitrary[Double])
    }
  }
  implicit val arbitraryDoubleArraySlice: Arbitrary[ArraySlice[Double]] = arbSlice(arbitraryDoubleArray)

  type ArrayNumBytes[A] = (Array[A], Int)

  implicit val arbitraryLongArrayNumBytes: Arbitrary[ArrayNumBytes[Long]] = Arbitrary {
    for {
      xs <- arbitraryLongArray.arbitrary
      from <- Gen.choose(0, xs.length)
      until <- Gen.choose(from, xs.length)
      bytes <- Gen.choose(0, 8)
    } yield (xs.slice(from, until), bytes)
  }

  implicit val arbitraryByteStringBuilder: Arbitrary[ByteStringBuilder] = Arbitrary(ByteString.newBuilder)

  def likeVector(bs: ByteString)(body: IndexedSeq[Byte] => Any): Boolean = {
    val vec = Vector(bs: _*)
    val a = body(bs)
    val b = body(vec)
    val result = a == b
    if (!result) {
      println(s"$bs => $a != $vec => $b")
    }
    result
  }

  def likeVectors(bsA: ByteString, bsB: ByteString)(body: (IndexedSeq[Byte], IndexedSeq[Byte]) => Any): Boolean = {
    val vecA = Vector(bsA: _*)
    val vecB = Vector(bsB: _*)
    body(bsA, bsB) == body(vecA, vecB)
  }

  @nowarn
  def likeVecIt(bs: ByteString)(body: BufferedIterator[Byte] => Any, strict: Boolean = true): Boolean = {
    val bsIterator = bs.iterator
    val vecIterator = Vector(bs: _*).iterator.buffered
    (body(bsIterator) == body(vecIterator)) &&
    (!strict || (bsIterator.toSeq == vecIterator.toSeq))
  }

  @nowarn
  def likeVecIts(a: ByteString, b: ByteString)(
      body: (BufferedIterator[Byte], BufferedIterator[Byte]) => Any,
      strict: Boolean = true): Boolean = {
    val (bsAIt, bsBIt) = (a.iterator, b.iterator)
    val (vecAIt, vecBIt) = (Vector(a: _*).iterator.buffered, Vector(b: _*).iterator.buffered)
    (body(bsAIt, bsBIt) == body(vecAIt, vecBIt)) &&
    (!strict || (bsAIt.toSeq -> bsBIt.toSeq) == (vecAIt.toSeq -> vecBIt.toSeq))
  }

  def likeVecBld(body: Builder[Byte, _] => Unit): Boolean = {
    val bsBuilder = ByteString.newBuilder
    val vecBuilder = Vector.newBuilder[Byte]

    body(bsBuilder)
    body(vecBuilder)

    bsBuilder.result() == vecBuilder.result()
  }

  def testShortDecoding(slice: ByteStringSlice, byteOrder: ByteOrder): Boolean = {
    val elemSize = 2
    val (bytes, from, until) = slice
    val (n, a, b) = (bytes.length / elemSize, from / elemSize, until / elemSize)
    val reference = new Array[Short](n)
    bytes.asByteBuffer.order(byteOrder).asShortBuffer.get(reference, 0, n)
    val input = bytes.iterator
    val decoded = new Array[Short](n)
    for (i <- 0 until a) decoded(i) = input.getShort(byteOrder)
    input.getShorts(decoded, a, b - a)(byteOrder)
    for (i <- b until n) decoded(i) = input.getShort(byteOrder)
    (decoded.toSeq == reference.toSeq) && (input.toSeq == bytes.drop(n * elemSize))
  }

  def testIntDecoding(slice: ByteStringSlice, byteOrder: ByteOrder): Boolean = {
    val elemSize = 4
    val (bytes, from, until) = slice
    val (n, a, b) = (bytes.length / elemSize, from / elemSize, until / elemSize)
    val reference = new Array[Int](n)
    bytes.asByteBuffer.order(byteOrder).asIntBuffer.get(reference, 0, n)
    val input = bytes.iterator
    val decoded = new Array[Int](n)
    for (i <- 0 until a) decoded(i) = input.getInt(byteOrder)
    input.getInts(decoded, a, b - a)(byteOrder)
    for (i <- b until n) decoded(i) = input.getInt(byteOrder)
    (decoded.toSeq == reference.toSeq) && (input.toSeq == bytes.drop(n * elemSize))
  }

  def testLongDecoding(slice: ByteStringSlice, byteOrder: ByteOrder): Boolean = {
    val elemSize = 8
    val (bytes, from, until) = slice
    val (n, a, b) = (bytes.length / elemSize, from / elemSize, until / elemSize)
    val reference = new Array[Long](n)
    bytes.asByteBuffer.order(byteOrder).asLongBuffer.get(reference, 0, n)
    val input = bytes.iterator
    val decoded = new Array[Long](n)
    for (i <- 0 until a) decoded(i) = input.getLong(byteOrder)
    input.getLongs(decoded, a, b - a)(byteOrder)
    for (i <- b until n) decoded(i) = input.getLong(byteOrder)
    (decoded.toSeq == reference.toSeq) && (input.toSeq == bytes.drop(n * elemSize))
  }

  def testFloatDecoding(slice: ByteStringSlice, byteOrder: ByteOrder): Boolean = {
    val elemSize = 4
    val (bytes, from, until) = slice
    val (n, a, b) = (bytes.length / elemSize, from / elemSize, until / elemSize)
    val reference = new Array[Float](n)
    bytes.asByteBuffer.order(byteOrder).asFloatBuffer.get(reference, 0, n)
    val input = bytes.iterator
    val decoded = new Array[Float](n)
    for (i <- 0 until a) decoded(i) = input.getFloat(byteOrder)
    input.getFloats(decoded, a, b - a)(byteOrder)
    for (i <- b until n) decoded(i) = input.getFloat(byteOrder)
    ((decoded.toSeq.map(floatToRawIntBits)) == (reference.toSeq.map(floatToRawIntBits))) &&
    (input.toSeq == bytes.drop(n * elemSize))
  }

  def testDoubleDecoding(slice: ByteStringSlice, byteOrder: ByteOrder): Boolean = {
    val elemSize = 8
    val (bytes, from, until) = slice
    val (n, a, b) = (bytes.length / elemSize, from / elemSize, until / elemSize)
    val reference = new Array[Double](n)
    bytes.asByteBuffer.order(byteOrder).asDoubleBuffer.get(reference, 0, n)
    val input = bytes.iterator
    val decoded = new Array[Double](n)
    for (i <- 0 until a) decoded(i) = input.getDouble(byteOrder)
    input.getDoubles(decoded, a, b - a)(byteOrder)
    for (i <- b until n) decoded(i) = input.getDouble(byteOrder)
    ((decoded.toSeq.map(doubleToRawLongBits)) == (reference.toSeq.map(doubleToRawLongBits))) &&
    (input.toSeq == bytes.drop(n * elemSize))
  }

  def testShortEncoding(slice: ArraySlice[Short], byteOrder: ByteOrder): Boolean = {
    val elemSize = 2
    val (data, from, to) = slice
    val reference = new Array[Byte](data.length * elemSize)
    ByteBuffer.wrap(reference).order(byteOrder).asShortBuffer.put(data)
    val builder = ByteString.newBuilder
    for (i <- 0 until from) builder.putShort(data(i))(byteOrder)
    builder.putShorts(data, from, to - from)(byteOrder)
    for (i <- to until data.length) builder.putShort(data(i))(byteOrder)
    reference.toSeq == builder.result()
  }

  def testIntEncoding(slice: ArraySlice[Int], byteOrder: ByteOrder): Boolean = {
    val elemSize = 4
    val (data, from, to) = slice
    val reference = new Array[Byte](data.length * elemSize)
    ByteBuffer.wrap(reference).order(byteOrder).asIntBuffer.put(data)
    val builder = ByteString.newBuilder
    for (i <- 0 until from) builder.putInt(data(i))(byteOrder)
    builder.putInts(data, from, to - from)(byteOrder)
    for (i <- to until data.length) builder.putInt(data(i))(byteOrder)
    reference.toSeq == builder.result()
  }

  def testLongEncoding(slice: ArraySlice[Long], byteOrder: ByteOrder): Boolean = {
    val elemSize = 8
    val (data, from, to) = slice
    val reference = new Array[Byte](data.length * elemSize)
    ByteBuffer.wrap(reference).order(byteOrder).asLongBuffer.put(data)
    val builder = ByteString.newBuilder
    for (i <- 0 until from) builder.putLong(data(i))(byteOrder)
    builder.putLongs(data, from, to - from)(byteOrder)
    for (i <- to until data.length) builder.putLong(data(i))(byteOrder)
    reference.toSeq == builder.result()
  }

  def testLongPartEncoding(anb: ArrayNumBytes[Long], byteOrder: ByteOrder): Boolean = {
    val elemSize = 8
    val (data, nBytes) = anb

    val reference = new Array[Byte](data.length * elemSize)
    ByteBuffer.wrap(reference).order(byteOrder).asLongBuffer.put(data)
    val builder = ByteString.newBuilder
    for (i <- 0 until data.length) builder.putLongPart(data(i), nBytes)(byteOrder)

    reference.zipWithIndex
      .collect({ // Since there is no partial put on LongBuffer, we need to collect only the interesting bytes
        case (r, i) if byteOrder == ByteOrder.LITTLE_ENDIAN && i % elemSize < nBytes            => r
        case (r, i) if byteOrder == ByteOrder.BIG_ENDIAN && i % elemSize >= (elemSize - nBytes) => r
      })
      .toSeq == builder.result()
  }

  def testFloatEncoding(slice: ArraySlice[Float], byteOrder: ByteOrder): Boolean = {
    val elemSize = 4
    val (data, from, to) = slice
    val reference = new Array[Byte](data.length * elemSize)
    ByteBuffer.wrap(reference).order(byteOrder).asFloatBuffer.put(data)
    val builder = ByteString.newBuilder
    for (i <- 0 until from) builder.putFloat(data(i))(byteOrder)
    builder.putFloats(data, from, to - from)(byteOrder)
    for (i <- to until data.length) builder.putFloat(data(i))(byteOrder)
    reference.toSeq == builder.result()
  }

  def testDoubleEncoding(slice: ArraySlice[Double], byteOrder: ByteOrder): Boolean = {
    val elemSize = 8
    val (data, from, to) = slice
    val reference = new Array[Byte](data.length * elemSize)
    ByteBuffer.wrap(reference).order(byteOrder).asDoubleBuffer.put(data)
    val builder = ByteString.newBuilder
    for (i <- 0 until from) builder.putDouble(data(i))(byteOrder)
    builder.putDoubles(data, from, to - from)(byteOrder)
    for (i <- to until data.length) builder.putDouble(data(i))(byteOrder)
    reference.toSeq == builder.result()
  }

  "ByteString1" must {
    "drop" in {
      ByteString1.empty.drop(-1) should ===(ByteString(""))
      ByteString1.empty.drop(0) should ===(ByteString(""))
      ByteString1.empty.drop(1) should ===(ByteString(""))
      ByteString1.fromString("a").drop(-1) should ===(ByteString("a"))
      ByteString1.fromString("a").drop(0) should ===(ByteString("a"))
      ByteString1.fromString("a").drop(1) should ===(ByteString(""))
      ByteString1.fromString("a").drop(2) should ===(ByteString(""))
      ByteString1.fromString("abc").drop(-1) should ===(ByteString("abc"))
      ByteString1.fromString("abc").drop(0) should ===(ByteString("abc"))
      ByteString1.fromString("abc").drop(1) should ===(ByteString("bc"))
      ByteString1.fromString("abc").drop(2) should ===(ByteString("c"))
      ByteString1.fromString("abc").drop(3) should ===(ByteString(""))
      ByteString1.fromString("abc").drop(4) should ===(ByteString(""))
      ByteString1.fromString("0123456789").drop(1).take(2) should ===(ByteString("12"))
      ByteString1.fromString("0123456789").drop(5).take(4).drop(1).take(2) should ===(ByteString("67"))
    }
    "dropRight" in {
      ByteString1.empty.dropRight(-1) should ===(ByteString(""))
      ByteString1.empty.dropRight(0) should ===(ByteString(""))
      ByteString1.empty.dropRight(1) should ===(ByteString(""))
      ByteString1.fromString("a").dropRight(-1) should ===(ByteString("a"))
      ByteString1.fromString("a").dropRight(0) should ===(ByteString("a"))
      ByteString1.fromString("a").dropRight(1) should ===(ByteString(""))
      ByteString1.fromString("a").dropRight(2) should ===(ByteString(""))
      ByteString1.fromString("abc").dropRight(-1) should ===(ByteString("abc"))
      ByteString1.fromString("abc").dropRight(0) should ===(ByteString("abc"))
      ByteString1.fromString("abc").dropRight(1) should ===(ByteString("ab"))
      ByteString1.fromString("abc").dropRight(2) should ===(ByteString("a"))
      ByteString1.fromString("abc").dropRight(3) should ===(ByteString(""))
      ByteString1.fromString("abc").dropRight(4) should ===(ByteString(""))
      ByteString1.fromString("0123456789").dropRight(1).take(2) should ===(ByteString("01"))
      ByteString1.fromString("0123456789").dropRight(5).take(4).drop(1).take(2) should ===(ByteString("12"))
    }
    "take" in {
      ByteString1.empty.take(-1) should ===(ByteString(""))
      ByteString1.empty.take(0) should ===(ByteString(""))
      ByteString1.empty.take(1) should ===(ByteString(""))
      ByteString1.fromString("a").take(1) should ===(ByteString("a"))
      ByteString1.fromString("ab").take(-1) should ===(ByteString(""))
      ByteString1.fromString("ab").take(0) should ===(ByteString(""))
      ByteString1.fromString("ab").take(1) should ===(ByteString("a"))
      ByteString1.fromString("ab").take(2) should ===(ByteString("ab"))
      ByteString1.fromString("ab").take(3) should ===(ByteString("ab"))
      ByteString1.fromString("0123456789").take(3).drop(1) should ===(ByteString("12"))
      ByteString1.fromString("0123456789").take(10).take(8).drop(3).take(5) should ===(ByteString("34567"))
    }
    "copyToArray" in {
      val byteString = ByteString1(Array[Byte](1, 2, 3, 4, 5), startIndex = 1, length = 3)
      def verify(f: Array[Byte] => Unit)(expected: Byte*): Unit = {
        val array = Array.fill[Byte](3)(0)
        f(array)
        array should ===(expected.toArray)
      }

      verify(byteString.copyToArray(_, 0, 1))(2, 0, 0)
      verify(byteString.copyToArray(_, 1, 1))(0, 2, 0)
      verify(byteString.copyToArray(_, 2, 1))(0, 0, 2)
      verify(byteString.copyToArray(_, 3, 1))(0, 0, 0)
      verify(byteString.copyToArray(_, 0, 2))(2, 3, 0)
      verify(byteString.copyToArray(_, 1, 2))(0, 2, 3)
      verify(byteString.copyToArray(_, 2, 2))(0, 0, 2)
      verify(byteString.copyToArray(_, 3, 2))(0, 0, 0)
      verify(byteString.copyToArray(_, 0, 3))(2, 3, 4)
      verify(byteString.copyToArray(_, 1, 3))(0, 2, 3)
      verify(byteString.copyToArray(_, 2, 3))(0, 0, 2)
      verify(byteString.copyToArray(_, 3, 3))(0, 0, 0)
    }
  }
  "ByteString1C" must {
    "drop" in {
      ByteString1C.fromString("").drop(-1) should ===(ByteString(""))
      ByteString1C.fromString("").drop(0) should ===(ByteString(""))
      ByteString1C.fromString("").drop(1) should ===(ByteString(""))
      ByteString1C.fromString("a").drop(-1) should ===(ByteString("a"))
      ByteString1C.fromString("a").drop(0) should ===(ByteString("a"))
      ByteString1C.fromString("a").drop(1) should ===(ByteString(""))
      ByteString1C.fromString("a").drop(2) should ===(ByteString(""))
      ByteString1C.fromString("abc").drop(-1) should ===(ByteString("abc"))
      ByteString1C.fromString("abc").drop(0) should ===(ByteString("abc"))
      ByteString1C.fromString("abc").drop(1) should ===(ByteString("bc"))
      ByteString1C.fromString("abc").drop(2) should ===(ByteString("c"))
      ByteString1C.fromString("abc").drop(3) should ===(ByteString(""))
      ByteString1C.fromString("abc").drop(4) should ===(ByteString(""))
      ByteString1C.fromString("0123456789").drop(1).take(2) should ===(ByteString("12"))
      ByteString1C.fromString("0123456789").drop(5).take(4).drop(1).take(2) should ===(ByteString("67"))
    }
    "dropRight" in {
      ByteString1C.fromString("").dropRight(-1) should ===(ByteString(""))
      ByteString1C.fromString("").dropRight(0) should ===(ByteString(""))
      ByteString1C.fromString("").dropRight(1) should ===(ByteString(""))
      ByteString1C.fromString("a").dropRight(-1) should ===(ByteString("a"))
      ByteString1C.fromString("a").dropRight(0) should ===(ByteString("a"))
      ByteString1C.fromString("a").dropRight(1) should ===(ByteString(""))
      ByteString1C.fromString("a").dropRight(2) should ===(ByteString(""))
      ByteString1C.fromString("abc").dropRight(-1) should ===(ByteString("abc"))
      ByteString1C.fromString("abc").dropRight(0) should ===(ByteString("abc"))
      ByteString1C.fromString("abc").dropRight(1) should ===(ByteString("ab"))
      ByteString1C.fromString("abc").dropRight(2) should ===(ByteString("a"))
      ByteString1C.fromString("abc").dropRight(3) should ===(ByteString(""))
      ByteString1C.fromString("abc").dropRight(4) should ===(ByteString(""))
      ByteString1C.fromString("0123456789").dropRight(1).take(2) should ===(ByteString("01"))
      ByteString1C.fromString("0123456789").dropRight(5).take(4).drop(1).take(2) should ===(ByteString("12"))
    }
    "take" in {
      ByteString1.fromString("abcdefg").drop(1).take(0) should ===(ByteString(""))
      ByteString1.fromString("abcdefg").drop(1).take(-1) should ===(ByteString(""))
      ByteString1.fromString("abcdefg").drop(1).take(-2) should ===(ByteString(""))
      ByteString1.fromString("abcdefg").drop(2) should ===(ByteString("cdefg"))
      ByteString1.fromString("abcdefg").drop(2).take(1) should ===(ByteString("c"))
    }
    "copyToArray" in {
      val byteString = ByteString1C(Array[Byte](1, 2, 3))
      def verify(f: Array[Byte] => Unit)(expected: Byte*): Unit = {
        val array = Array.fill[Byte](3)(0)
        f(array)
        array should ===(expected.toArray)
      }

      verify(byteString.copyToArray(_, 0, 1))(1, 0, 0)
      verify(byteString.copyToArray(_, 1, 1))(0, 1, 0)
      verify(byteString.copyToArray(_, 2, 1))(0, 0, 1)
      verify(byteString.copyToArray(_, 3, 1))(0, 0, 0)
      verify(byteString.copyToArray(_, 0, 2))(1, 2, 0)
      verify(byteString.copyToArray(_, 1, 2))(0, 1, 2)
      verify(byteString.copyToArray(_, 2, 2))(0, 0, 1)
      verify(byteString.copyToArray(_, 3, 2))(0, 0, 0)
      verify(byteString.copyToArray(_, 0, 3))(1, 2, 3)
      verify(byteString.copyToArray(_, 1, 3))(0, 1, 2)
      verify(byteString.copyToArray(_, 2, 3))(0, 0, 1)
      verify(byteString.copyToArray(_, 3, 3))(0, 0, 0)
    }
  }
  "ByteStrings" must {
    "drop" in {
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("")).drop(Int.MinValue) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("")).drop(-1) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("")).drop(0) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("")).drop(1) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("")).drop(Int.MaxValue) should ===(ByteString(""))

      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).drop(Int.MinValue) should ===(
        ByteString("a"))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).drop(-1) should ===(ByteString("a"))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).drop(0) should ===(ByteString("a"))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).drop(1) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).drop(2) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).drop(Int.MaxValue) should ===(ByteString(""))

      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("a")).drop(Int.MinValue) should ===(
        ByteString("a"))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("a")).drop(-1) should ===(ByteString("a"))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("a")).drop(0) should ===(ByteString("a"))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("a")).drop(1) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("a")).drop(2) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("a")).drop(Int.MaxValue) should ===(ByteString(""))

      val bss =
        ByteStrings(Vector(ByteString1.fromString("a"), ByteString1.fromString("bc"), ByteString1.fromString("def")))

      bss.drop(Int.MinValue) should ===(ByteString("abcdef"))
      bss.drop(-1) should ===(ByteString("abcdef"))
      bss.drop(0) should ===(ByteString("abcdef"))
      bss.drop(1) should ===(ByteString("bcdef"))
      bss.drop(2) should ===(ByteString("cdef"))
      bss.drop(3) should ===(ByteString("def"))
      bss.drop(4) should ===(ByteString("ef"))
      bss.drop(5) should ===(ByteString("f"))
      bss.drop(6) should ===(ByteString(""))
      bss.drop(7) should ===(ByteString(""))
      bss.drop(Int.MaxValue) should ===(ByteString(""))

      ByteString("0123456789").drop(5).take(2) should ===(ByteString("56"))
      ByteString("0123456789").drop(5).drop(3).take(1) should ===(ByteString("8"))
      (ByteString1C.fromString("a") ++ ByteString1.fromString("bc")).drop(2) should ===(ByteString("c"))
    }
    "dropRight" in {
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("")).dropRight(Int.MinValue) should ===(
        ByteString(""))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("")).dropRight(-1) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("")).dropRight(0) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("")).dropRight(1) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("")).dropRight(Int.MaxValue) should ===(
        ByteString(""))

      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).dropRight(Int.MinValue) should ===(
        ByteString("a"))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).dropRight(-1) should ===(ByteString("a"))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).dropRight(0) should ===(ByteString("a"))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).dropRight(1) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).dropRight(2) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).dropRight(Int.MaxValue) should ===(
        ByteString(""))

      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("a")).dropRight(Int.MinValue) should ===(
        ByteString("a"))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("a")).dropRight(-1) should ===(ByteString("a"))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("a")).dropRight(0) should ===(ByteString("a"))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("a")).dropRight(1) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("a")).dropRight(2) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("a")).dropRight(Int.MaxValue) should ===(
        ByteString(""))

      val bss =
        ByteStrings(Vector(ByteString1.fromString("a"), ByteString1.fromString("bc"), ByteString1.fromString("def")))

      bss.dropRight(Int.MinValue) should ===(ByteString("abcdef"))
      bss.dropRight(-1) should ===(ByteString("abcdef"))
      bss.dropRight(0) should ===(ByteString("abcdef"))
      bss.dropRight(1) should ===(ByteString("abcde"))
      bss.dropRight(2) should ===(ByteString("abcd"))
      bss.dropRight(3) should ===(ByteString("abc"))
      bss.dropRight(4) should ===(ByteString("ab"))
      bss.dropRight(5) should ===(ByteString("a"))
      bss.dropRight(6) should ===(ByteString(""))
      bss.dropRight(7) should ===(ByteString(""))
      bss.dropRight(Int.MaxValue) should ===(ByteString(""))

      ByteString("0123456789").dropRight(5).take(2) should ===(ByteString("01"))
      ByteString("0123456789").dropRight(5).drop(3).take(1) should ===(ByteString("3"))
      (ByteString1C.fromString("a") ++ ByteString1.fromString("bc")).dropRight(2) should ===(ByteString("a"))
    }
    "slice" in {
      ByteStrings(ByteString1.fromString(""), ByteString1.fromString("a")).slice(1, 1) should ===(ByteString(""))
      // We explicitly test all edge cases to always test them, refs bug #21237
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).slice(-10, 10) should ===(ByteString("a"))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).slice(-10, 0) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).slice(-10, 1) should ===(ByteString("a"))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).slice(0, 1) should ===(ByteString("a"))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).slice(0, 10) should ===(ByteString("a"))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).slice(1, 10) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).slice(1, -2) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).slice(-10, -100) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).slice(-100, -10) should ===(ByteString(""))
      // Get an empty if `from` is greater then `until`
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).slice(1, 0) should ===(ByteString(""))

      ByteStrings(ByteString1.fromString("ab"), ByteString1.fromString("cd")).slice(2, 2) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("ab"), ByteString1.fromString("cd")).slice(2, 3) should ===(ByteString("c"))
      ByteStrings(ByteString1.fromString("ab"), ByteString1.fromString("cd")).slice(2, 4) should ===(ByteString("cd"))
      ByteStrings(ByteString1.fromString("ab"), ByteString1.fromString("cd")).slice(3, 4) should ===(ByteString("d"))
      // Can obtain expected results from 6 basic patterns
      ByteStrings(ByteString1.fromString("ab"), ByteString1.fromString("cd")).slice(-10, 10) should ===(
        ByteString("abcd"))
      ByteStrings(ByteString1.fromString("ab"), ByteString1.fromString("cd")).slice(-10, 0) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("ab"), ByteString1.fromString("cd")).slice(-10, 4) should ===(
        ByteString("abcd"))
      ByteStrings(ByteString1.fromString("ab"), ByteString1.fromString("cd")).slice(0, 4) should ===(ByteString("abcd"))
      ByteStrings(ByteString1.fromString("ab"), ByteString1.fromString("cd")).slice(1, -2) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("ab"), ByteString1.fromString("cd")).slice(0, 10) should ===(
        ByteString("abcd"))
      ByteStrings(ByteString1.fromString("ab"), ByteString1.fromString("cd")).slice(-10, -100) should ===(
        ByteString(""))
      ByteStrings(ByteString1.fromString("ab"), ByteString1.fromString("cd")).slice(-100, -10) should ===(
        ByteString(""))

      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).slice(1, -2) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).slice(-10, -100) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("")).slice(-100, -10) should ===(ByteString(""))

      // various edge cases using raw ByteString1
      ByteString1.fromString("cd").slice(100, 10) should ===(ByteString(""))
      ByteString1.fromString("cd").slice(100, 1000) should ===(ByteString(""))
      ByteString1.fromString("cd").slice(-10, -5) should ===(ByteString(""))
      ByteString1.fromString("cd").slice(-2, -5) should ===(ByteString(""))
      ByteString1.fromString("cd").slice(-2, 1) should ===(ByteString("c"))
      ByteString1.fromString("abcd").slice(1, -1) should ===(ByteString(""))

      // Get an empty if `from` is greater than `until`
      ByteStrings(ByteString1.fromString("ab"), ByteString1.fromString("cd")).slice(4, 0) should ===(ByteString(""))
    }
    "take" in {
      ByteString.empty.take(-1) should ===(ByteString(""))
      ByteString.empty.take(0) should ===(ByteString(""))
      ByteString.empty.take(1) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("bc")).drop(1).take(0) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("bc")).drop(1).take(-1) should ===(ByteString(""))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("bc")).drop(1).take(-2) should ===(ByteString(""))
      (ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("bc")) ++ ByteString1.fromString("defg"))
        .drop(2) should ===(ByteString("cdefg"))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("bc")).drop(2).take(1) should ===(ByteString("c"))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("bc")).take(100) should ===(ByteString("abc"))
      ByteStrings(ByteString1.fromString("a"), ByteString1.fromString("bc")).drop(1).take(100) should ===(
        ByteString("bc"))
    }
    "indexOf" in {
      ByteString.empty.indexOf(5) should ===(-1)
      val byteString1 = ByteString1.fromString("abc")
      byteString1.indexOf('a') should ===(0)
      byteString1.indexOf('b') should ===(1)
      byteString1.indexOf('c') should ===(2)
      byteString1.indexOf('d') should ===(-1)

      val byteStrings = ByteStrings(ByteString1.fromString("abc"), ByteString1.fromString("efg"))
      byteStrings.indexOf('a') should ===(0)
      byteStrings.indexOf('c') should ===(2)
      byteStrings.indexOf('d') should ===(-1)
      byteStrings.indexOf('e') should ===(3)
      byteStrings.indexOf('f') should ===(4)
      byteStrings.indexOf('g') should ===(5)

      val compact = byteStrings.compact
      compact.indexOf('a') should ===(0)
      compact.indexOf('c') should ===(2)
      compact.indexOf('d') should ===(-1)
      compact.indexOf('e') should ===(3)
      compact.indexOf('f') should ===(4)
      compact.indexOf('g') should ===(5)

    }
    "indexOf from offset" in {
      ByteString.empty.indexOf(5, -1) should ===(-1)
      ByteString.empty.indexOf(5, 0) should ===(-1)
      ByteString.empty.indexOf(5, 1) should ===(-1)
      val byteString1 = ByteString1.fromString("abc")
      byteString1.indexOf('d', -1) should ===(-1)
      byteString1.indexOf('d', 0) should ===(-1)
      byteString1.indexOf('d', 1) should ===(-1)
      byteString1.indexOf('d', 4) should ===(-1)
      byteString1.indexOf('a', -1) should ===(0)
      byteString1.indexOf('a', 0) should ===(0)
      byteString1.indexOf('a', 1) should ===(-1)

      val byteStrings = ByteStrings(ByteString1.fromString("abc"), ByteString1.fromString("efg"))
      byteStrings.indexOf('c', -1) should ===(2)
      byteStrings.indexOf('c', 0) should ===(2)
      byteStrings.indexOf('c', 2) should ===(2)
      byteStrings.indexOf('c', 3) should ===(-1)

      byteStrings.indexOf('e', -1) should ===(3)
      byteStrings.indexOf('e', 0) should ===(3)
      byteStrings.indexOf('e', 1) should ===(3)
      byteStrings.indexOf('e', 4) should ===(-1)
      byteStrings.indexOf('e', 6) should ===(-1)

      byteStrings.indexOf('g', -1) should ===(5)
      byteStrings.indexOf('g', 0) should ===(5)
      byteStrings.indexOf('g', 1) should ===(5)
      byteStrings.indexOf('g', 4) should ===(5)
      byteStrings.indexOf('g', 5) should ===(5)
      byteStrings.indexOf('g', 6) should ===(-1)

      val compact = byteStrings.compact
      compact.indexOf('c', -1) should ===(2)
      compact.indexOf('c', 0) should ===(2)
      compact.indexOf('c', 2) should ===(2)
      compact.indexOf('c', 3) should ===(-1)

      compact.indexOf('e', -1) should ===(3)
      compact.indexOf('e', 0) should ===(3)
      compact.indexOf('e', 1) should ===(3)
      compact.indexOf('e', 4) should ===(-1)
      compact.indexOf('e', 6) should ===(-1)

      compact.indexOf('g', -1) should ===(5)
      compact.indexOf('g', 0) should ===(5)
      compact.indexOf('g', 1) should ===(5)
      compact.indexOf('g', 4) should ===(5)
      compact.indexOf('g', 5) should ===(5)
      compact.indexOf('g', 6) should ===(-1)
    }
    "copyToArray" in {
      val byteString = ByteString(1, 2) ++ ByteString(3) ++ ByteString(4)

      def verify(f: Array[Byte] => Unit)(expected: Byte*): Unit = {
        val array = Array.fill[Byte](3)(0)
        f(array)
        array should ===(expected.toArray)
      }

      verify(byteString.copyToArray(_, 0, 1))(1, 0, 0)
      verify(byteString.copyToArray(_, 1, 1))(0, 1, 0)
      verify(byteString.copyToArray(_, 2, 1))(0, 0, 1)
      verify(byteString.copyToArray(_, 3, 1))(0, 0, 0)
      verify(byteString.copyToArray(_, 0, 2))(1, 2, 0)
      verify(byteString.copyToArray(_, 1, 2))(0, 1, 2)
      verify(byteString.copyToArray(_, 2, 2))(0, 0, 1)
      verify(byteString.copyToArray(_, 3, 2))(0, 0, 0)
      verify(byteString.copyToArray(_, 0, 3))(1, 2, 3)
      verify(byteString.copyToArray(_, 1, 3))(0, 1, 2)
      verify(byteString.copyToArray(_, 2, 3))(0, 0, 1)
      verify(byteString.copyToArray(_, 3, 3))(0, 0, 0)
    }
  }

  "A ByteString" must {
    "have correct size" when {
      "concatenating" in { check((a: ByteString, b: ByteString) => (a ++ b).size == a.size + b.size) }
      "dropping" in { check((a: ByteString, b: ByteString) => (a ++ b).drop(b.size).size == a.size) }
      "taking" in { check((a: ByteString, b: ByteString) => (a ++ b).take(a.size) == a) }
      "takingRight" in { check((a: ByteString, b: ByteString) => (a ++ b).takeRight(b.size) == b) }
      "dropping then taking" in {
        check((a: ByteString, b: ByteString) => (b ++ a ++ b).drop(b.size).take(a.size) == a)
      }
      "droppingRight" in { check((a: ByteString, b: ByteString) => (b ++ a ++ b).drop(b.size).dropRight(b.size) == a) }
    }

    "be sequential" when {
      "taking" in { check((a: ByteString, b: ByteString) => (a ++ b).take(a.size) == a) }
      "dropping" in { check((a: ByteString, b: ByteString) => (a ++ b).drop(a.size) == b) }
    }

    "be equal to the original" when {
      "compacting" in {
        check { (xs: ByteString) =>
          val ys = xs.compact; (xs == ys) && ys.isCompact
        }
      }
      "recombining" in {
        check { (xs: ByteString, from: Int, until: Int) =>
          val (tmp, c) = xs.splitAt(until)
          val (a, b) = tmp.splitAt(from)
          (a ++ b ++ c) == xs
        }
      }
      def excerciseRecombining(xs: ByteString, from: Int, until: Int) = {
        val (tmp, c) = xs.splitAt(until)
        val (a, b) = tmp.splitAt(from)
        (a ++ b ++ c) should ===(xs)
      }
      "recombining - edge cases" in {
        excerciseRecombining(
          ByteStrings(Vector(ByteString1(Array[Byte](1)), ByteString1(Array[Byte](2)))),
          -2147483648,
          112121212)
        excerciseRecombining(ByteStrings(Vector(ByteString1(Array[Byte](100)))), 0, 2)
        excerciseRecombining(ByteStrings(Vector(ByteString1(Array[Byte](100)))), -2147483648, 2)
        excerciseRecombining(ByteStrings(Vector(ByteString1.fromString("ab"), ByteString1.fromString("cd"))), 0, 1)
        excerciseRecombining(ByteString1.fromString("abc").drop(1).take(1), -324234, 234232)
        excerciseRecombining(ByteString("a"), 0, 2147483647)
        excerciseRecombining(
          ByteStrings(Vector(ByteString1.fromString("ab"), ByteString1.fromString("cd"))).drop(2),
          2147483647,
          1)
        excerciseRecombining(ByteString1.fromString("ab").drop1(1), Int.MaxValue, Int.MaxValue)
      }
    }

    "behave as expected" when {
      "created from and decoding to String" in {
        check { (s: String) =>
          ByteString(s, "UTF-8").decodeString("UTF-8") == s
        }
      }

      "taking its own length" in {
        check { (b: ByteString) =>
          b.take(b.length) eq b
        }
      }

      "created from and decoding to Base64" in {
        check { (a: ByteString) =>
          val encoded = a.encodeBase64
          encoded == ByteString(java.util.Base64.getEncoder.encode(a.toArray)) &&
          encoded.decodeBase64 == a
        }
      }

      "compacting" in {
        check { (a: ByteString) =>
          val wasCompact = a.isCompact
          val b = a.compact
          ((!wasCompact) || (b eq a)) &&
          (b == a) &&
          b.isCompact &&
          (b.compact eq b)
        }
      }

      "asByteBuffers" in {
        check { (a: ByteString) =>
          if (a.isCompact) a.asByteBuffers.size == 1 && a.asByteBuffers.head == a.asByteBuffer
          else a.asByteBuffers.size > 0
        }
        check { (a: ByteString) =>
          a.asByteBuffers.foldLeft(ByteString.empty) { (bs, bb) =>
            bs ++ ByteString(bb)
          } == a
        }
        check { (a: ByteString) =>
          a.asByteBuffers.forall(_.isReadOnly)
        }
        check { (a: ByteString) =>
          import scala.jdk.CollectionConverters._
          a.asByteBuffers.zip(a.getByteBuffers().asScala).forall(x => x._1 == x._2)
        }
      }

      "toString should start with ByteString(" in {
        check { (bs: ByteString) =>
          bs.toString.startsWith("ByteString(")
        }
      }
    }
    "behave like a Vector" when {
      "concatenating" in {
        check { (a: ByteString, b: ByteString) =>
          likeVectors(a, b) { _ ++ _ }
        }
      }

      "calling apply" in {
        check { (slice: ByteStringSlice) =>
          slice match {
            case (xs, i1, i2) =>
              likeVector(xs) { seq =>
                (if ((i1 >= 0) && (i1 < seq.length)) seq(i1) else 0, if ((i2 >= 0) && (i2 < seq.length)) seq(i2) else 0)
              }
          }
        }
      }

      "calling head" in {
        check { (a: ByteString) =>
          a.isEmpty || likeVector(a) { _.head }
        }
      }
      "calling tail" in {
        check { (a: ByteString) =>
          a.isEmpty || likeVector(a) { _.tail }
        }
      }
      "calling last" in {
        check { (a: ByteString) =>
          a.isEmpty || likeVector(a) { _.last }
        }
      }
      "calling init" in {
        check { (a: ByteString) =>
          a.isEmpty || likeVector(a) { _.init }
        }
      }
      "calling length" in {
        check { (a: ByteString) =>
          likeVector(a) { _.length }
        }
      }

      "calling span" in {
        check { (a: ByteString, b: Byte) =>
          likeVector(a)({ _.span(_ != b) match { case (a, b) => (a, b) } })
        }
      }

      "calling takeWhile" in {
        check { (a: ByteString, b: Byte) =>
          likeVector(a)({ _.takeWhile(_ != b) })
        }
      }
      "calling dropWhile" in {
        check { (a: ByteString, b: Byte) =>
          likeVector(a) { _.dropWhile(_ != b) }
        }
      }
      "calling indexWhere" in {
        check { (a: ByteString, b: Byte) =>
          likeVector(a) { _.indexWhere(_ == b) }
        }
      }
      "calling indexOf" in {
        check { (a: ByteString, b: Byte) =>
          likeVector(a) { _.indexOf(b) }
        }
      }
      // this actually behave weird for Vector and negative indexes - SI9936, fixed in Scala 2.12
      // so let's just skip negative indexes (doesn't make much sense anyway)
      "calling indexOf(elem, idx)" in {
        check { (a: ByteString, b: Byte, idx: Int) =>
          likeVector(a) { _.indexOf(b, math.max(0, idx)) }
        }
      }

      "calling foreach" in {
        check { (a: ByteString) =>
          likeVector(a) { it =>
            var acc = 0; it.foreach { acc += _ }; acc
          }
        }
      }
      "calling foldLeft" in {
        check { (a: ByteString) =>
          likeVector(a) { _.foldLeft(0) { _ + _ } }
        }
      }
      "calling toArray" in {
        check { (a: ByteString) =>
          likeVector(a) { _.toArray.toSeq }
        }
      }

      "calling slice" in {
        check { (slice: ByteStringSlice) =>
          slice match {
            case (xs, from, until) =>
              likeVector(xs)({
                _.slice(from, until)
              })
          }
        }
      }

      "calling take and drop" in {
        check { (slice: ByteStringSlice) =>
          slice match {
            case (xs, from, until) =>
              likeVector(xs)({
                _.drop(from).take(until - from)
              })
          }
        }
      }

      "calling grouped" in {
        check { (grouped: ByteStringGrouped) =>
          likeVector(grouped.bs) {
            _.grouped(grouped.size).toIndexedSeq
          }
        }
      }

      "calling copyToArray" in {
        check { (slice: ByteStringSlice) =>
          slice match {
            case (xs, from, until) =>
              likeVector(xs)({ it =>
                val array = new Array[Byte](xs.length)
                it.copyToArray(array, from, until)
                array.toSeq
              })
          }
        }
      }

      "calling indexWhere(p, idx)" in {
        check { (a: ByteString, b: Byte, idx: Int) =>
          likeVector(a)(_.indexWhere(_ == b, math.max(0, idx)))
        }
      }
    }

    "serialize correctly" when {
      // note that this is serialization with Java serialization
      // real serialization is in akka-remote

      "given all types of ByteString" in {
        check { (bs: ByteString) =>
          testSer(bs)
        }
      }

      "with a large concatenated bytestring" in {
        // coverage for #20901
        val original = ByteString(Array.fill[Byte](1000)(1)) ++ ByteString(Array.fill[Byte](1000)(2))

        deserialize(serialize(original)) shouldEqual original
      }
    }

    "unsafely wrap and unwrap bytes" in {
      // optimal case
      val bytes = Array.fill[Byte](100)(7)
      val bs = ByteString.fromArrayUnsafe(bytes)
      val bytes2 = bs.toArrayUnsafe()
      (bytes2 should be).theSameInstanceAs(bytes)

      val combinedBs = bs ++ bs
      val combinedBytes = combinedBs.toArrayUnsafe()
      combinedBytes should ===(bytes ++ bytes)
    }
  }

  "A ByteStringIterator" must {
    "behave like a buffered Vector Iterator" when {
      "concatenating" in {
        check { (a: ByteString, b: ByteString) =>
          likeVecIts(a, b) { (a, b) =>
            (a ++ b).toSeq
          }
        }
      }

      "calling head" in {
        check { (a: ByteString) =>
          a.isEmpty || likeVecIt(a) { _.head }
        }
      }
      "calling next" in {
        check { (a: ByteString) =>
          a.isEmpty || likeVecIt(a) { _.next() }
        }
      }
      "calling hasNext" in {
        check { (a: ByteString) =>
          likeVecIt(a) { _.hasNext }
        }
      }
      "calling length" in {
        check { (a: ByteString) =>
          likeVecIt(a)(_.length, strict = false)
        }
      }
      "calling duplicate" in {
        check { (a: ByteString) =>
          likeVecIt(a)({ _.duplicate match { case (a, b) => (a.toSeq, b.toSeq) } }, strict = false)
        }
      }

      // Have to used toList instead of toSeq here, iterator.span (new in
      // Scala-2.9) seems to be broken in combination with toSeq for the
      // scala.collection default Iterator (see Scala issue SI-5838).
      "calling span" in {
        check { (a: ByteString, b: Byte) =>
          likeVecIt(a)({ _.span(_ != b) match { case (a, b) => (a.toList, b.toList) } }, strict = false)
        }
      }

      "calling takeWhile" in {
        check { (a: ByteString, b: Byte) =>
          likeVecIt(a)({ _.takeWhile(_ != b).toSeq }, strict = false)
        }
      }
      "calling dropWhile" in {
        check { (a: ByteString, b: Byte) =>
          likeVecIt(a) { _.dropWhile(_ != b).toSeq }
        }
      }
      "calling indexWhere" in {
        check { (a: ByteString, b: Byte) =>
          likeVecIt(a) { _.indexWhere(_ == b) }
        }
      }
      "calling indexOf" in {
        check { (a: ByteString, b: Byte) =>
          likeVecIt(a) { _.indexOf(b) }
        }
      }
      "calling toSeq" in {
        check { (a: ByteString) =>
          likeVecIt(a) { _.toSeq }
        }
      }
      "calling foreach" in {
        check { (a: ByteString) =>
          likeVecIt(a) { it =>
            var acc = 0; it.foreach { acc += _ }; acc
          }
        }
      }
      "calling foldLeft" in {
        check { (a: ByteString) =>
          likeVecIt(a) { _.foldLeft(0) { _ + _ } }
        }
      }
      "calling toArray" in {
        check { (a: ByteString) =>
          likeVecIt(a) { _.toArray.toSeq }
        }
      }

      "calling slice" in {
        check { (slice: ByteStringSlice) =>
          slice match {
            case (xs, from, until) =>
              likeVecIt(xs)({
                _.slice(from, until).toSeq
              }, strict = false)
          }
        }
      }

      "calling take and drop" in {
        check { (slice: ByteStringSlice) =>
          slice match {
            case (xs, from, until) =>
              likeVecIt(xs)({
                _.drop(from).take(until - from).toSeq
              }, strict = false)
          }
        }
      }

      "calling copyToArray" in {
        check { (slice: ByteStringSlice) =>
          slice match {
            case (xs, from, until) =>
              likeVecIt(xs)({ it =>
                val array = new Array[Byte](xs.length)
                it.slice(from, until).copyToArray(array, from, until)
                array.toSeq
              }, strict = false)
          }
        }
      }
    }

    "function as expected" when {
      "getting Bytes, using getByte and getBytes" in {
        // mixing getByte and getBytes here for more rigorous testing
        check { (slice: ByteStringSlice) =>
          val (bytes, from, to) = slice
          val input = bytes.iterator
          val output = new Array[Byte](bytes.length)
          for (i <- 0 until from) output(i) = input.getByte
          input.getBytes(output, from, to - from)
          for (i <- to until bytes.length) output(i) = input.getByte
          (output.toSeq == bytes) && (input.isEmpty)
        }
      }

      "getting Bytes with a given length" in {
        check { (slice: ByteStringSlice) =>
          val (bytes, _, _) = slice
          val input = bytes.iterator
          (input.getBytes(bytes.length).toSeq == bytes) && input.isEmpty
        }
      }

      "getting ByteString with a given length" in {
        check { (slice: ByteStringSlice) =>
          val (bytes, _, _) = slice
          val input = bytes.iterator
          (input.getByteString(bytes.length) == bytes) && input.isEmpty
        }
      }

      "getting Bytes, using the InputStream wrapper" in {
        // combining skip and both read methods here for more rigorous testing
        check { (slice: ByteStringSlice) =>
          val (bytes, from, to) = slice
          val a = (0 max from) min bytes.length
          val b = (a max to) min bytes.length
          val input = bytes.iterator
          val output = new Array[Byte](bytes.length)

          input.asInputStream.skip(a)

          val toRead = b - a
          var (nRead, eof) = (0, false)
          while ((nRead < toRead) && !eof) {
            val n = input.asInputStream.read(output, a + nRead, toRead - nRead)
            if (n == -1) eof = true
            else nRead += n
          }
          if (eof) throw new RuntimeException("Unexpected EOF")

          for (i <- b until bytes.length) output(i) = input.asInputStream.read().toByte

          (output.toSeq.drop(a) == bytes.drop(a)) &&
          (input.asInputStream.read() == -1) &&
          ((output.length < 1) || (input.asInputStream.read(output, 0, 1) == -1))
        }
      }

      "calling copyToBuffer" in {
        check { (bytes: ByteString) =>
          import java.nio.ByteBuffer
          val buffer = ByteBuffer.allocate(bytes.size)
          bytes.copyToBuffer(buffer)
          buffer.flip()
          val array = new Array[Byte](bytes.size)
          buffer.get(array)
          bytes == array.toSeq
        }
      }

      "copying chunks to an array" in {
        val iterator = (ByteString("123") ++ ByteString("456")).iterator
        val array = Array.ofDim[Byte](6)
        iterator.copyToArray(array, 0, 2)
        iterator.copyToArray(array, 2, 2)
        iterator.copyToArray(array, 4, 2)
        assert(new String(array) === "123456")
      }

      "calling copyToArray with length passing end of destination" in {
        // Pre fix len passing the end of the destination would cause never ending loop inside iterator copyToArray
        val iterator = (ByteString(1, 2) ++ ByteString(3) ++ ByteString(4)).iterator
        val array = Array.fill[Byte](3)(0)
        iterator.copyToArray(array, 2, 2)
        array.toSeq should ===(Seq(0, 0, 1))
      }
    }

    "decode data correctly" when {
      "decoding Short in big-endian" in {
        check { (slice: ByteStringSlice) =>
          testShortDecoding(slice, BIG_ENDIAN)
        }
      }
      "decoding Short in little-endian" in {
        check { (slice: ByteStringSlice) =>
          testShortDecoding(slice, LITTLE_ENDIAN)
        }
      }
      "decoding Int in big-endian" in {
        check { (slice: ByteStringSlice) =>
          testIntDecoding(slice, BIG_ENDIAN)
        }
      }
      "decoding Int in little-endian" in {
        check { (slice: ByteStringSlice) =>
          testIntDecoding(slice, LITTLE_ENDIAN)
        }
      }
      "decoding Long in big-endian" in {
        check { (slice: ByteStringSlice) =>
          testLongDecoding(slice, BIG_ENDIAN)
        }
      }
      "decoding Long in little-endian" in {
        check { (slice: ByteStringSlice) =>
          testLongDecoding(slice, LITTLE_ENDIAN)
        }
      }
      "decoding Float in big-endian" in {
        check { (slice: ByteStringSlice) =>
          testFloatDecoding(slice, BIG_ENDIAN)
        }
      }
      "decoding Float in little-endian" in {
        check { (slice: ByteStringSlice) =>
          testFloatDecoding(slice, LITTLE_ENDIAN)
        }
      }
      "decoding Double in big-endian" in {
        check { (slice: ByteStringSlice) =>
          testDoubleDecoding(slice, BIG_ENDIAN)
        }
      }
      "decoding Double in little-endian" in {
        check { (slice: ByteStringSlice) =>
          testDoubleDecoding(slice, LITTLE_ENDIAN)
        }
      }
    }
  }

  "A ByteStringBuilder" must {
    "function like a VectorBuilder" when {
      "adding various contents using ++= and +=" in {
        check { (array1: Array[Byte], array2: Array[Byte], bs1: ByteString, bs2: ByteString, bs3: ByteString) =>
          likeVecBld { builder =>
            builder ++= array1
            bs1.foreach { b =>
              builder += b
            }
            builder ++= bs2
            bs3.foreach { b =>
              builder += b
            }
            builder ++= array2.toIndexedSeq
          }
        }
      }
    }
    "function as expected" when {
      "putting Bytes, using putByte and putBytes" in {
        // mixing putByte and putBytes here for more rigorous testing
        check { (slice: ArraySlice[Byte]) =>
          val (data, from, to) = slice
          val builder = ByteString.newBuilder
          for (i <- 0 until from) builder.putByte(data(i))
          builder.putBytes(data, from, to - from)
          for (i <- to until data.length) builder.putByte(data(i))
          data.toSeq == builder.result()
        }
      }

      "putting Bytes, using the OutputStream wrapper" in {
        // mixing the write methods here for more rigorous testing
        check { (slice: ArraySlice[Byte]) =>
          val (data, from, to) = slice
          val builder = ByteString.newBuilder
          for (i <- 0 until from) builder.asOutputStream.write(data(i).toInt)
          builder.asOutputStream.write(data, from, to - from)
          for (i <- to until data.length) builder.asOutputStream.write(data(i).toInt)
          data.toSeq == builder.result()
        }
      }
    }

    "encode data correctly" when {
      "encoding Short in big-endian" in {
        check { (slice: ArraySlice[Short]) =>
          testShortEncoding(slice, BIG_ENDIAN)
        }
      }
      "encoding Short in little-endian" in {
        check { (slice: ArraySlice[Short]) =>
          testShortEncoding(slice, LITTLE_ENDIAN)
        }
      }
      "encoding Int in big-endian" in {
        check { (slice: ArraySlice[Int]) =>
          testIntEncoding(slice, BIG_ENDIAN)
        }
      }
      "encoding Int in little-endian" in {
        check { (slice: ArraySlice[Int]) =>
          testIntEncoding(slice, LITTLE_ENDIAN)
        }
      }
      "encoding Long in big-endian" in {
        check { (slice: ArraySlice[Long]) =>
          testLongEncoding(slice, BIG_ENDIAN)
        }
      }
      "encoding Long in little-endian" in {
        check { (slice: ArraySlice[Long]) =>
          testLongEncoding(slice, LITTLE_ENDIAN)
        }
      }
      "encoding LongPart in big-endian" in {
        check { (slice: ArrayNumBytes[Long]) =>
          testLongPartEncoding(slice, BIG_ENDIAN)
        }
      }
      "encoding LongPart in little-endian" in {
        check { (slice: ArrayNumBytes[Long]) =>
          testLongPartEncoding(slice, LITTLE_ENDIAN)
        }
      }
      "encoding Float in big-endian" in {
        check { (slice: ArraySlice[Float]) =>
          testFloatEncoding(slice, BIG_ENDIAN)
        }
      }
      "encoding Float in little-endian" in {
        check { (slice: ArraySlice[Float]) =>
          testFloatEncoding(slice, LITTLE_ENDIAN)
        }
      }
      "encoding Double in big-endian" in {
        check { (slice: ArraySlice[Double]) =>
          testDoubleEncoding(slice, BIG_ENDIAN)
        }
      }
      "encoding Double in little-endian" in {
        check { (slice: ArraySlice[Double]) =>
          testDoubleEncoding(slice, LITTLE_ENDIAN)
        }
      }
    }

    "have correct empty info" when {
      "is empty" in {
        check { (a: ByteStringBuilder) =>
          a.isEmpty
        }
      }
      "is nonEmpty" in {
        check { (a: ByteStringBuilder) =>
          a.putByte(1.toByte)
          a.nonEmpty
        }
      }
    }

    // overloading issue #32272
    "support adding arbitrary sequences of bytes" in {
      val builder = new ByteStringBuilder()
      builder ++= Seq[Byte](1, 2, 3)
      builder.result() shouldEqual ByteString(1, 2, 3)
    }
  }
}
