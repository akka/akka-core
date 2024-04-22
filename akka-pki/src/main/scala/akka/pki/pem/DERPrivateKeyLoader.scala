/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pki.pem

import akka.annotation.ApiMayChange
import akka.pki.pem.PEMDecoder.DERData
import com.hierynomus.asn1.ASN1InputStream
import com.hierynomus.asn1.encodingrules.der.DERDecoder
import com.hierynomus.asn1.types.constructed.ASN1Sequence
import com.hierynomus.asn1.types.primitive.ASN1Integer
import com.hierynomus.asn1.types.primitive.ASN1ObjectIdentifier
import com.hierynomus.asn1.types.string.ASN1OctetString

import java.math.BigInteger
import java.security.AlgorithmParameters
import java.security.KeyFactory
import java.security.PrivateKey
import java.security.spec.ECGenParameterSpec
import java.security.spec.ECParameterSpec
import java.security.spec.ECPrivateKeySpec
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.RSAMultiPrimePrivateCrtKeySpec
import java.security.spec.RSAOtherPrimeInfo
import java.security.spec.RSAPrivateCrtKeySpec
import scala.jdk.CollectionConverters._

final class PEMLoadingException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(msg: String) = this(msg, null)
}

object DERPrivateKeyLoader {

  /**
   * Java API: Converts the DER payload in the first private key entry in the given  [[PEMDecoder.DERData]] into a [[java.security.PrivateKey]].
   * The received DER data must be a valid PKCS#1 (identified in PEM as "RSA PRIVATE KEY") or non-encrypted PKCS#8 (identified
   * in PEM as "PRIVATE KEY" or "EC PRIVATE KEY").
   *
   * @throws PEMLoadingException when the no entry in `derData` is a supported format
   */
  @ApiMayChange
  @throws[PEMLoadingException]("when the `derData` is for an unsupported format")
  def load(derData: java.util.List[DERData]): PrivateKey =
    load(derData.asScala.toVector)

  /**
   * Scala API: Converts the DER payload in the first private key entry in the given  [[PEMDecoder.DERData]] into a [[java.security.PrivateKey]].
   * The received DER data must be a valid PKCS#1 (identified in PEM as "RSA PRIVATE KEY") or non-encrypted PKCS#8 (identified
   * in PEM as "PRIVATE KEY" or "EC PRIVATE KEY").
   *
   * @throws PEMLoadingException when the no entry in `derData` is a supported format
   */
  @ApiMayChange
  @throws[PEMLoadingException]("when the `derData` is for an unsupported format")
  def load(derData: Seq[DERData]): PrivateKey = {
    derData
      .find(entry =>
        entry.label == "RSA PRIVATE KEY" || entry.label == "PRIVATE KEY" || entry.label == "EC PRIVATE KEY")
      .map(load)
      .getOrElse(throw new PEMLoadingException(
        s"None of the provided entries [${derData.map(_.label).mkString("'", "', '", "'")}] is a supported type of private key"))
  }

  /**
   * Converts the DER payload in [[PEMDecoder.DERData]] into a [[java.security.PrivateKey]]. The received DER
   * data must be a valid PKCS#1 PKCS#1 (identified in PEM as "RSA PRIVATE KEY") or non-encrypted PKCS#8 (identified
   * * in PEM as "PRIVATE KEY" or "EC PRIVATE KEY").
   *
   * @throws PEMLoadingException when the `derData` is for an unsupported format
   */
  @ApiMayChange
  @throws[PEMLoadingException]("when the `derData` is for an unsupported format")
  def load(derData: DERData): PrivateKey = {
    derData.label match {
      case "RSA PRIVATE KEY" =>
        loadPkcs1PrivateKey(derData.bytes)
      case "PRIVATE KEY" =>
        loadPkcs8PrivateKey(derData.bytes)
      case "EC PRIVATE KEY" =>
        loadEcPrivateKey(derData.bytes)
      case unknown =>
        throw new PEMLoadingException(s"Don't know how to read a private key from PEM data with label [$unknown]")
    }
  }

  private def loadPkcs1PrivateKey(bytes: Array[Byte]) = {
    val derInputStream = new ASN1InputStream(new DERDecoder, bytes)
    // Here's the specification: https://tools.ietf.org/html/rfc3447#appendix-A.1.2
    val sequence = {
      try {
        derInputStream.readObject[ASN1Sequence]()
      } finally {
        derInputStream.close()
      }
    }
    val version = getInteger(sequence, 0, "version").intValueExact()
    if (version < 0 || version > 1) {
      throw new IllegalArgumentException(s"Unsupported PKCS1 version: $version")
    }
    val modulus = getInteger(sequence, 1, "modulus")
    val publicExponent = getInteger(sequence, 2, "publicExponent")
    val privateExponent = getInteger(sequence, 3, "privateExponent")
    val prime1 = getInteger(sequence, 4, "prime1")
    val prime2 = getInteger(sequence, 5, "prime2")
    val exponent1 = getInteger(sequence, 6, "exponent1")
    val exponent2 = getInteger(sequence, 7, "exponent2")
    val coefficient = getInteger(sequence, 8, "coefficient")

    val keySpec = if (version == 0) {
      new RSAPrivateCrtKeySpec(
        modulus,
        publicExponent,
        privateExponent,
        prime1,
        prime2,
        exponent1,
        exponent2,
        coefficient)
    } else {
      // Does anyone even use multi-primes? Who knows, maybe this code will never be used. Anyway, I guess it will work,
      // the spec isn't exactly complicated.
      val otherPrimeInfosSequence = getSequence(sequence, 9, "otherPrimeInfos")
      val otherPrimeInfos = (for (i <- 0 until otherPrimeInfosSequence.size()) yield {
        val name = s"otherPrimeInfos[$i]"
        val seq = getSequence(otherPrimeInfosSequence, i, name)
        val prime = getInteger(seq, 0, s"$name.prime")
        val exponent = getInteger(seq, 1, s"$name.exponent")
        val coefficient = getInteger(seq, 2, s"$name.coefficient")
        new RSAOtherPrimeInfo(prime, exponent, coefficient)
      }).toArray
      new RSAMultiPrimePrivateCrtKeySpec(
        modulus,
        publicExponent,
        privateExponent,
        prime1,
        prime2,
        exponent1,
        exponent2,
        coefficient,
        otherPrimeInfos)
    }

    val keyFactory = KeyFactory.getInstance("RSA")
    keyFactory.generatePrivate(keySpec)
  }

  private def getInteger(sequence: ASN1Sequence, index: Int, name: String): BigInteger = {
    sequence.get(index) match {
      case integer: ASN1Integer => integer.getValue
      case other =>
        throw new IllegalArgumentException(s"Expected integer tag for $name at index $index, but got: ${other.getTag}")
    }
  }

  private def getSequence(sequence: ASN1Sequence, index: Int, name: String): ASN1Sequence = {
    sequence.get(index) match {
      case seq: ASN1Sequence => seq
      case other =>
        throw new IllegalArgumentException(s"Expected sequence tag for $name at index $index, but got: ${other.getTag}")
    }
  }

  private def getOctetString(sequence: ASN1Sequence, index: Int, name: String): ASN1OctetString = {
    sequence.get(index) match {
      case octetString: ASN1OctetString => octetString
      case other =>
        throw new IllegalArgumentException(
          s"Expected octet string tag for $name at index $index, but got: ${other.getTag}")
    }
  }

  private def loadPkcs8PrivateKey(bytes: Array[Byte]) = {
    val keySpec = new PKCS8EncodedKeySpec(bytes)

    val derInputStream = new ASN1InputStream(new DERDecoder, bytes)
    val sequence = {
      try {
        derInputStream.readObject[ASN1Sequence]()
      } finally {
        derInputStream.close()
      }
    }

    /*
     PrivateKeyInfo ::= SEQUENCE {
        version                   Version,
        privateKeyAlgorithm       PrivateKeyAlgorithmIdentifier,
        privateKey                PrivateKey,
        attributes           [0]  IMPLICIT Attributes OPTIONAL }

      Version ::= INTEGER

      PrivateKeyAlgorithmIdentifier ::= AlgorithmIdentifier

      PrivateKey ::= OCTET STRING

      Attributes ::= SET OF Attribute

      AlgorithmIdentifier  ::=  SEQUENCE  {
         algorithm   OBJECT IDENTIFIER,
         parameters  ANY DEFINED BY algorithm OPTIONAL
     }
     */
    val version = getInteger(sequence, 0, "version")
    require(version == BigInteger.ZERO, s"Exepcted version 0, but file is of format version $version")
    val javaPrivateKeyAlgorithmName = sequence.get(1) match {
      case privateKeyAlgorithmIdentifier: ASN1Sequence =>
        privateKeyAlgorithmIdentifier.get(0) match {
          case oid: ASN1ObjectIdentifier =>
            oid.getValue match {
              case "1.2.840.113549.1.1.1" => "rsa"
              case "1.2.840.10045.2.1"    => "ec"
              case "1.3.101.110"          => "X25519"
              case "1.3.101.111"          => "x448"
              case "1.3.101.112"          => "ed25519"
              case "1.3.101.113"          => "ed448"
              case unknown                => throw new IllegalArgumentException(s"Unknown algorithm idenfifier [$unknown]")
            }
          case unexpected =>
            throw new IllegalArgumentException(
              s"Unexpected type of the algorithm, expected object idenfier, was: [${unexpected.getClass}]")
        }
      case unexpected =>
        throw new IllegalArgumentException(
          s"Unexpected type of the privateKeyAlgorithm, expected object idenfier, was: [${unexpected.getClass}]")
    }

    val keyFactory = KeyFactory.getInstance(javaPrivateKeyAlgorithmName)
    keyFactory.generatePrivate(keySpec)
  }

  private def loadEcPrivateKey(bytes: Array[Byte]) = {
    val derInputStream = new ASN1InputStream(new DERDecoder, bytes)

    // https://www.rfc-editor.org/rfc/rfc5915#section-3
    //  ECPrivateKey ::= SEQUENCE {
    //     version        INTEGER { ecPrivkeyVer1(1) } (ecPrivkeyVer1),
    //     privateKey     OCTET STRING,
    //     parameters [0] ECParameters {{ NamedCurve }} OPTIONAL,
    //     publicKey  [1] BIT STRING OPTIONAL
    //   }
    val sequence = {
      try {
        derInputStream.readObject[ASN1Sequence]()
      } finally {
        derInputStream.close()
      }
    }
    val version = getInteger(sequence, 0, "version")
    require(version == BigInteger.ONE, s"Version expected to be 1 but was $version")
    val privateKey = getOctetString(sequence, 1, "privateKey")
    // OS2IP
    val privateKeyBigInteger = new BigInteger(1, privateKey.getValue)

    // ECParameters: https://www.rfc-editor.org/rfc/rfc5480#appendix-A
    // not actually optional, only the NamedCurve CHOICE allowed
    val parameters = sequence.get(2).getValue match {
      case oid: ASN1ObjectIdentifier => oid
      case other                     => throw new IllegalArgumentException(s"Unexpected type of the parameters: ${other.getClass}")
    }
    // mapping from object id to named curve
    // https://www.rfc-editor.org/rfc/rfc5480#section-2.1.1.1
    val namedCurve = parameters.getValue match {
      case "1.2.840.10045.3.1.1" => "secp192r1"
      case "1.3.132.0.1"         => "sect163k1"
      case "1.3.132.0.15"        => "sect163r2"
      case "1.3.132.0.33"        => "secp224r1"
      case "1.3.132.0.26"        => "sect233k1"
      case "1.3.132.0.27"        => "sect233r1"
      case "1.2.840.10045.3.1.7" => "secp256r1"
      case "1.3.132.0.16"        => "sect283k1"
      case "1.3.132.0.17"        => "sect283r1"
      case "1.3.132.0.34"        => "secp384r1"
      case "1.3.132.0.36"        => "sect409k1"
      case "1.3.132.0.37"        => "sect409r1"
      case "1.3.132.0.35"        => "secp521r1"
      case "1.3.132.0.38"        => "sect571k1"
      case "1.3.132.0.39"        => "sect571r1"
      case other                 => throw new IllegalArgumentException(s"Unknown named curve object id [$other]")
    }

    val parameterSpec = new ECGenParameterSpec(namedCurve)
    val algorithmParameters = AlgorithmParameters.getInstance("EC")
    algorithmParameters.init(parameterSpec)
    val ecParameters = algorithmParameters.getParameterSpec(classOf[ECParameterSpec])
    val keySpec = new ECPrivateKeySpec(privateKeyBigInteger, ecParameters)
    val keyFactory = KeyFactory.getInstance("EC")
    keyFactory.generatePrivate(keySpec)
  }

}
