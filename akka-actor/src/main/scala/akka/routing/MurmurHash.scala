/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2003-2011, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    https://scala-lang.org/              **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

/**
 * An implementation of Austin Appleby's MurmurHash 3.0 algorithm
 *  (32 bit version); reference: https://github.com/aappleby/smhasher
 *
 *  This is the hash used by collections and case classes (including
 *  tuples).
 *
 *  @author  Rex Kerr
 *  @version 2.9
 *  @since   2.9
 */
package akka.routing

import java.lang.Integer.{ rotateLeft => rotl }

import scala.annotation.nowarn

/**
 * An object designed to generate well-distributed non-cryptographic
 *  hashes.  It is designed to hash a collection of integers; along with
 *  the integers to hash, it generates two magic streams of integers to
 *  increase the distribution of repetitive input sequences.  Thus,
 *  three methods need to be called at each step (to start and to
 *  incorporate a new integer) to update the values.  Only one method
 *  needs to be called to finalize the hash.
 */
object MurmurHash {
  // Magic values used for MurmurHash's 32 bit hash.
  // Don't change these without consulting a hashing expert!
  final private val visibleMagic: Int = 0x971e137b
  final private val hiddenMagicA: Int = 0x95543787
  final private val hiddenMagicB: Int = 0x2ad7eb25
  final private val visibleMixer: Int = 0x52dce729
  final private val hiddenMixerA: Int = 0x7b7d159c
  final private val hiddenMixerB: Int = 0x6bce6396
  final private val finalMixer1: Int = 0x85ebca6b
  final private val finalMixer2: Int = 0xc2b2ae35

  // Arbitrary values used for hashing certain classes
  final private val seedString: Int = 0xf7ca7fd2
  final private val seedArray: Int = 0x3c074a61

  /** The first 23 magic integers from the first stream are stored here */
  private val storedMagicA: Array[Int] =
    Iterator.iterate(hiddenMagicA)(nextMagicA).take(23).toArray

  /** The first 23 magic integers from the second stream are stored here */
  private val storedMagicB: Array[Int] =
    Iterator.iterate(hiddenMagicB)(nextMagicB).take(23).toArray

  /** Begin a new hash with a seed value. */
  def startHash(seed: Int): Int = seed ^ visibleMagic

  /** The initial magic integers in the first stream. */
  def startMagicA: Int = hiddenMagicA

  /** The initial magic integer in the second stream. */
  def startMagicB: Int = hiddenMagicB

  /**
   * Incorporates a new value into an existing hash.
   *
   * @param   hash    the prior hash value
   * @param  value    the new value to incorporate
   * @param magicA    a magic integer from the stream
   * @param magicB    a magic integer from a different stream
   * @return          the updated hash value
   */
  def extendHash(hash: Int, value: Int, magicA: Int, magicB: Int): Int =
    (hash ^ rotl(value * magicA, 11) * magicB) * 3 + visibleMixer

  /** Given a magic integer from the first stream, compute the next */
  def nextMagicA(magicA: Int): Int = magicA * 5 + hiddenMixerA

  /** Given a magic integer from the second stream, compute the next */
  def nextMagicB(magicB: Int): Int = magicB * 5 + hiddenMixerB

  /** Once all hashes have been incorporated, this performs a final mixing */
  def finalizeHash(hash: Int): Int = {
    var i = (hash ^ (hash >>> 16))
    i *= finalMixer1
    i ^= (i >>> 13)
    i *= finalMixer2
    i ^= (i >>> 16)
    i
  }

  /** Compute a high-quality hash of an array */
  def arrayHash[@specialized T](a: Array[T]): Int = {
    var h = startHash(a.length * seedArray)
    var c = hiddenMagicA
    var k = hiddenMagicB
    var j = 0
    while (j < a.length) {
      h = extendHash(h, a(j).##, c, k)
      c = nextMagicA(c)
      k = nextMagicB(k)
      j += 1
    }
    finalizeHash(h)
  }

  /** Compute a high-quality hash of a string */
  def stringHash(s: String): Int = {
    var h = startHash(s.length * seedString)
    var c = hiddenMagicA
    var k = hiddenMagicB
    var j = 0
    while (j + 1 < s.length) {
      val i = (s.charAt(j) << 16) + s.charAt(j + 1)
      h = extendHash(h, i, c, k)
      c = nextMagicA(c)
      k = nextMagicB(k)
      j += 2
    }
    if (j < s.length) h = extendHash(h, s.charAt(j), c, k)
    finalizeHash(h)
  }

  /**
   * Compute a hash that is symmetric in its arguments--that is,
   *  where the order of appearance of elements does not matter.
   *  This is useful for hashing sets, for example.
   */
  @nowarn("msg=deprecated")
  def symmetricHash[T](xs: IterableOnce[T], seed: Int): Int = {
    var a, b, n = 0
    var c = 1
    xs.foreach(i => {
      val h = i.##
      a += h
      b ^= h
      if (h != 0) c *= h
      n += 1
    })
    var h = startHash(seed * n)
    h = extendHash(h, a, storedMagicA(0), storedMagicB(0))
    h = extendHash(h, b, storedMagicA(1), storedMagicB(1))
    h = extendHash(h, c, storedMagicA(2), storedMagicB(2))
    finalizeHash(h)
  }
}
