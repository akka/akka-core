/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp

import java.io.ByteArrayOutputStream
import java.security.NoSuchAlgorithmException
import java.security.SecureRandom
import java.util.zip.GZIPOutputStream

import akka.event.NoMarkerLogging
import akka.testkit.AkkaSpec

class SecureRandomFactorySHA1Spec extends SecureRandomFactorySpec("SHA1PRNG")
class SecureRandomFactoryNativePRNGSpec extends SecureRandomFactorySpec("NativePRNG")
class SecureRandomFactoryJVMChoiceSpec extends SecureRandomFactorySpec("SecureRandom")
class SecureRandomFactoryBlankSpec extends SecureRandomFactorySpec("")
class SecureRandomFactoryInvalidPRNGSpec extends SecureRandomFactorySpec("InvalidPRNG")

abstract class SecureRandomFactorySpec(alg: String) extends AkkaSpec {
  var prng: SecureRandom = null

  def isSupported: Boolean = {
    try {
      prng = SecureRandomFactory.createSecureRandom(alg, NoMarkerLogging)
      prng.nextInt() // Has to work
      val sRng = alg
      if (prng.getAlgorithm != sRng && sRng != "")
        throw new NoSuchAlgorithmException(sRng)
      true
    } catch {
      case e: NoSuchAlgorithmException =>
        info(e.toString)
        false
    }
  }

  s"Artery's Secure Random support ($alg)" must {
    if (isSupported) {
      "generate random" in {
        val bytes = Array.ofDim[Byte](16)
        // Reproducer of the specific issue described at
        // https://doc.akka.io/libraries/akka-core/current/security/2018-08-29-aes-rng.html
        // awaitAssert just in case we are very unlucky to get same sequence more than once
        awaitAssert {
          val randomBytes = List
            .fill(10) {
              prng.nextBytes(bytes)
              bytes.toVector
            }
            .toSet
          randomBytes.size should ===(10)
        }
      }

      "have random numbers that are not compressable, because then they are not random" in {
        val randomData = new Array[Byte](1024 * 1024)
        prng.nextBytes(randomData)

        val baos = new ByteArrayOutputStream()
        val gzipped = new GZIPOutputStream(baos)
        try gzipped.write(randomData)
        finally gzipped.close()

        val compressed = baos.toByteArray
        // random data should not be compressible
        // Another reproducer of https://doc.akka.io/libraries/akka-core/current/security/2018-08-29-aes-rng.html
        // with the broken implementation the compressed size was <5k
        compressed.size should be > randomData.length
      }

    } else {
      s"not be run when the $alg PRNG provider is not supported by the platform this test is currently being executed on" in {
        pending
      }
    }
  }
}
