/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns

import akka.util.OptionVal

/**
 * DNS Record Type
 */
final case class RecordType(code: Short, name: String)

object RecordType {

  /**
   * array for fast lookups by id
   * wasteful, but we get trivial indexing into it for lookup
   */
  private final val lookupTable = Array.ofDim[RecordType](256)

  private[akka] def lookup(code: Int): RecordType = lookupTable(code)
  def apply(id: Short): OptionVal[RecordType] = {
    if (id < 1 || id > 255) OptionVal.None
    else OptionVal(RecordType.lookup(id))
  }

  private def register(t: RecordType): RecordType = {
    lookupTable(t.code) = t
    t
  }

  /** A host address */
  final val A: RecordType = register(RecordType(1, "A"))

  /** An authoritative name server */
  final val NS: RecordType = register(RecordType(2, "NS"))

  /** A mail destination (Obsolete - use MX) */
  final val MD: RecordType = register(RecordType(3, "MD"))

  /** A mail forwarder (Obsolete - use MX) */
  final val MF: RecordType = register(RecordType(4, "MF"))

  /** the canonical name for an alias */
  final val CNAME: RecordType = register(RecordType(5, "CNAME"))

  /** marks the start of a zone of authority */
  final val SOA: RecordType = register(RecordType(6, "SOA"))

  /** A mailbox domain name (EXPERIMENTAL) */
  final val MB: RecordType = register(RecordType(7, "MB"))

  /** A mail group member (EXPERIMENTAL) */
  final val MG: RecordType = register(RecordType(8, "MG"))

  /** A mail rename domain name (EXPERIMENTAL) */
  final val MR: RecordType = register(RecordType(9, "MR"))

  /** A null RR (EXPERIMENTAL) */
  final val NULL: RecordType = register(RecordType(10, "NULL"))

  /** A well known service description */
  final val WKS: RecordType = register(RecordType(11, "WKS"))

  /** A domain name pointer */
  final val PTR: RecordType = register(RecordType(12, "PTR"))

  /** host information */
  final val HINFO: RecordType = register(RecordType(13, "HINFO"))

  /** mailbox or mail list information */
  final val MINFO: RecordType = register(RecordType(14, "MINFO"))

  /** mail exchange */
  final val MX: RecordType = register(RecordType(15, "MX"))

  /** text strings */
  final val TXT: RecordType = register(RecordType(16, "TXT"))

  /** The AAAA resource record type is a record specific to the Internet class that stores a single IPv6 address. */
  // See: https://tools.ietf.org/html/rfc3596
  final val AAAA: RecordType = register(RecordType(28, "AAAA"))

  /**
   * The SRV RR allows administrators to use several servers for a single
   * domain, to move services from host to host with little fuss, and to
   * designate some hosts as primary servers for a service and others as
   * backups.
   */
  // See: https://tools.ietf.org/html/rfc2782
  final val SRV: RecordType = register(RecordType(33, "SRV"))

  final val AXFR: RecordType = register(RecordType(252, "AXFR"))
  final val MAILB: RecordType = register(RecordType(253, "MAILB"))
  final val MAILA: RecordType = register(RecordType(254, "MAILA"))
  final val WILDCARD: RecordType = register(RecordType(255, "WILDCARD"))
}
