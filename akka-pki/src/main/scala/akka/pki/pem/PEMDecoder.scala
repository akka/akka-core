/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pki.pem

import java.util.Base64

import akka.annotation.ApiMayChange

/**
 * Decodes lax PEM encoded data, according to
 *
 * https://tools.ietf.org/html/rfc7468
 */
object PEMDecoder {

  // I believe this regex matches the RFC7468 Lax ABNF semantics  jkhdft exactly.
  private val PEMRegex = {
    // Luckily, Java Pattern's \s matches the RFCs W ABNF expression perfectly
    // (space, tab, carriage return, line feed, form feed, vertical tab)

    // The variables here are named to match the expressions in the RFC7468 ABNF
    // description. The content of the regex may not match the structure of the
    // expression because sometimes there are nicer way to do things in regexes.

    // All printable ASCII characters minus hyphen
    val labelchar = """[\p{Print}&&[^-]]"""
    // Starts and finishes with a labelchar, with as many label chars and hyphens or
    // spaces in between, but no double spaces or hyphens, also may be empty.
    val label = raw"""(?:$labelchar(?:[\- ]?$labelchar)*)?"""
    // capturing group so we can extract the label
    val preeb = raw"""-----BEGIN ($label)-----"""
    // we don't extract the end label because the RFC says we can ignore it (it
    // doesn't have to match the begin label)
    val posteb = raw"""-----END $label-----"""
    // Any of the base64 chars (alphanum, +, /) and whitespace, followed by at most 2
    // padding characters, separated by zero to many whitespace characters
    val laxbase64text = """[A-Za-z0-9\+/\s]*(?:=\s*){0,2}"""

    val laxtextualmessage = raw"""\s*$preeb($laxbase64text)$posteb\s*"""

    laxtextualmessage.r
  }

  /**
   * Decodes the first certificate or private key entry in a PEM String into an identifier and the
   * DER bytes of the content.
   *
   * See https://tools.ietf.org/html/rfc7468 and https://en.wikipedia.org/wiki/Privacy-Enhanced_Mail
   *
   * @param pemData the PEM data (pre-eb, base64-MIME data and ponst-eb)
   * @return the decoded bytes and the content type.
   */
  @throws[PEMLoadingException](
    "If the `pemData` is not valid PEM format (according to https://tools.ietf.org/html/rfc7468).")
  @ApiMayChange
  def decode(pemData: String): DERData = {
    PEMRegex
      .findAllMatchIn(pemData)
      .find(regexMatch => regexMatch.group(1).contains("PRIVATE KEY") || regexMatch.group(1).contains("CERTIFICATE"))
      .map { privateKeyRegexMatch =>
        try {
          new DERData(privateKeyRegexMatch.group(1), Base64.getMimeDecoder.decode(privateKeyRegexMatch.group(2)))
        } catch {
          case iae: IllegalArgumentException =>
            throw new PEMLoadingException(
              s"Error decoding base64 data from PEM data (note: expected MIME-formatted Base64)",
              iae)
        }
      }
      .getOrElse(throw new PEMLoadingException(
        "Could not find a private key, a certificate or the given string is not PEM encoded data."))
  }

  @ApiMayChange
  final class DERData(val label: String, val bytes: Array[Byte])

}
