/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

/**
 * Base62 encoder/decoder.
 *
 * Unlike an encoding like Base64, a perfect Base62 encoding never aligns with the bytes - there are no whole numbers
 * for which you can say "for every n bytes, there are m characters output." This is because 62 and 256 share no common
 * exponents. So you can't perfectly break the input up into blocks, and encode it one block at a time, like you can
 * with Base64.
 *
 * Instead, we approximate it. We break the raw bytes into blocks of 8 bytes, and encoded blocks of 11 base62 digits.
 * This size was chosen because 62^^11 is greater than, but somewhat close to logarithmically, 256^^8. Because it's
 * greater than, it guarantees that every 8 byte block value can fit in 11 base62 characters, while the closeness
 * minimises the overhead due to values of 11 base62 characters that are too big to map to 8 bytes. I checked block
 * sizes up to 10, and found this yielded the best efficiency. Also, 8 is the longest input block size that allows us
 * to do all the arithmetic using primitive data types (in this case, unsigned long), rather than having to use a big
 * integer type, which would impact performance and make our code more complex.
 *
 * So, our encoded string is 11/8 (1.375) times longer than the input bytes, which is slightly worse than the optimal
 * base62 encoding efficiency of 8/log2(62), ie 1.344 times longer than the input bytes. But, implementing the optimal
 * encoding requires dividing the entire input byte array by 62 over and over, which would mean our encoding would run
 * in O(n2) rather than O(n).
 *
 * This does mean certain input strings, containing only valid base62 characters, may not be possible to decode. This
 * is also true with base64, but such corruptions can only happen in the last block, if the last block is one character
 * long. Whereas such corruptions can happen in any block in this encoding, for example, a block of only z's is invalid
 * anywhere in the encoded form.
 *
 * The encoder is reasonably performant. I haven't done any fine tunings or micro-benchmarks of it, but I have taken
 * care to ensure it does run in O(n), and avoided unnecessary buffer copies where possible. For the purposes of license
 * encoding, its runtime is negligible compared to the cryptographic signing and verification operations.
 *
 * There are other Base62 encoders out there, and they use a number of different incompatible approaches. Keybase has
 * a library called saltpack which contains a Base62 encoder. It uses the same block encoding approach, but with 32
 * byte blocks. However, its encoder is flexible enough to allow creating an encoder with any arbitrary block length.
 * This implementation has been verified to be compatible with the Keybase saltpack BaseX encoder for Base62 with a
 * block size of 8.
 */
private[akka] object Base62 {
  // This implementation cannot handle a raw block size greater than 8 because it reads the raw blocks into a long.
  private final val RawBlockSize = 8
  private final val EncodedBlockSize = 11
  // Technically, this implementation could be used for any sized encoding up to 62, just by changing this.
  private final val Base = 62

  import java.lang.{ Long => jLong }
  // The highest value a block can be before adding any digit will cause it to overflow
  private final val blockOverflowLimit = jLong.divideUnsigned(-1, 62)

  private def encodeDigit(digit: Long): Byte =
    (digit match {
      case d if d < 10 => '0' + d
      case u if u < 36 => 'A' - 10 + u
      case l           => 'a' - 36 + l
    }).asInstanceOf[Byte]

  private def decodeDigit(digit: Char): Int = digit match {
    case d if d >= '0' && d <= '9' => d - '0'
    case u if u >= 'A' && u <= 'Z' => u - 'A' + 10
    case l if l >= 'a' && l <= 'z' => l - 'a' + 36
    case invalid                   => throw new Base62EncodingException(s"Invalid base62 character: $invalid")
  }

  /**
   * Encode the given byte array into a string.
   */
  def encode(raw: Array[Byte]): String = {
    // Work out the block parameters, including how many blocks there are, whether there's a last partial block, and
    // how big it is, both for raw and encoded forms.
    val fullBlocks = raw.length / RawBlockSize
    val lastRawBlockSize = raw.length % RawBlockSize
    val totalBlocks = if (lastRawBlockSize == 0) fullBlocks else fullBlocks + 1
    val lastEncodedBlockSize = math.ceil(lastRawBlockSize.toDouble / RawBlockSize * EncodedBlockSize).toInt

    val encoded = new Array[Byte](fullBlocks * EncodedBlockSize + lastEncodedBlockSize)
    for (i <- 0 until totalBlocks) {
      // This is an unsigned long, we need to make sure all operations done to it operate in an unsigned fashion.
      // Hence below we use java.lang.Long.*Unsigned operations.
      var block = 0L
      val partialBlock = i == totalBlocks - 1 && lastRawBlockSize > 0
      val rawBlockSize = if (partialBlock) lastRawBlockSize else RawBlockSize
      for (j <- i * RawBlockSize until i * RawBlockSize + rawBlockSize) {
        block = (block << 8) | (raw(j) & 0xff)
      }
      val encodedBlockSize = if (partialBlock) lastEncodedBlockSize else EncodedBlockSize
      for (j <- i * EncodedBlockSize + encodedBlockSize - 1 to i * EncodedBlockSize by -1) {
        val digit = jLong.remainderUnsigned(block, Base)
        encoded(j) = encodeDigit(digit)
        block = jLong.divideUnsigned(block, Base)
      }
    }
    new String(encoded, "utf-8")
  }

  private def computeLastRawBlockSize(lastEncodedBlockSize: Int): Int =
    math.floor(lastEncodedBlockSize.toDouble / EncodedBlockSize * RawBlockSize).toInt

  /**
   * Decode the given encoded string into a byte array.
   */
  def decode(encoded: String): Array[Byte] = {
    // Work out the block parameters, including how many blocks there are, whether there's a last partial block, and
    // how big it is, both for the encoded and raw forms.
    val fullBlocks = encoded.length / EncodedBlockSize
    val lastEncodedBlockSize = encoded.length % EncodedBlockSize
    val totalBlocks = if (lastEncodedBlockSize == 0) fullBlocks else fullBlocks + 1
    val lastRawBlockSize = computeLastRawBlockSize(lastEncodedBlockSize)

    // Need to validate that the size of the last block is valid, not all last block sizes are valid. It's invalid if
    // one less than the encoded last block size would have the same raw block size.
    if (lastEncodedBlockSize != 0) {
      if (lastRawBlockSize == computeLastRawBlockSize(lastEncodedBlockSize - 1)) {
        throw new Base62EncodingException("Invalid base62 length")
      }
    }
    val raw = new Array[Byte](fullBlocks * RawBlockSize + lastRawBlockSize)
    for (i <- 0 until totalBlocks) {
      val partialBlock = i == totalBlocks - 1 && lastEncodedBlockSize > 0
      val encodedBlockSize = if (partialBlock) lastEncodedBlockSize else EncodedBlockSize
      // This is an unsigned long, we need to make sure all operations done to it operate in an unsigned fashion.
      // Multiply/add operate identically for signed/unsigned, but the logical bitshift (>>>) rather than arithmetic
      // bitshift (>>) is important.
      var block = 0L
      for (j <- i * EncodedBlockSize until i * EncodedBlockSize + encodedBlockSize) {
        // There are two possibilities for overflow, first is that multiplying by the base will trigger an overflow,
        // second is adding the digit will trigger an overflow. We need to check for both. Multiplying by the base
        // triggering an overflow can only be checked beforehand
        if (jLong.compareUnsigned(block, blockOverflowLimit) > 0) {
          throw new Base62EncodingException("Invalid base62 encoding")
        }
        val newBlock = block * Base + decodeDigit(encoded.charAt(j))
        // Now detect if the addition caused an overflow, because we're only adding a small value, an overflow can be
        // detected by checking if the new value is less than the first
        if (jLong.compareUnsigned(newBlock, block) < 0) {
          throw new Base62EncodingException("Invalid base62 encoding")
        }
        block = newBlock
      }
      val rawBlockSize = if (partialBlock) lastRawBlockSize else RawBlockSize
      for (j <- i * RawBlockSize + rawBlockSize - 1 to i * RawBlockSize by -1) {
        raw(j) = (block & 0xFF).asInstanceOf[Byte]
        block = block >>> 8
      }
      // Check for an invalid last block
      if (partialBlock && block != 0) {
        throw new Base62EncodingException("Invalid base62 encoding")
      }
    }
    raw
  }

  class Base62EncodingException(msg: String) extends RuntimeException(msg)

}
