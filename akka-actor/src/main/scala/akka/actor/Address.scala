/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor
import java.net.MalformedURLException
import java.net.URI
import java.net.URISyntaxException
import java.util.Optional
import scala.annotation.tailrec
import scala.collection.immutable
import scala.jdk.OptionConverters._
import akka.annotation.InternalApi

import scala.annotation.nowarn

/**
 * The address specifies the physical location under which an Actor can be
 * reached. Examples are local addresses, identified by the ActorSystem’s
 * name, and remote addresses, identified by protocol, host and port.
 *
 * This class is final to allow use as a case class (copy method etc.); if
 * for example a remote transport would want to associate additional
 * information with an address, then this must be done externally.
 *
 * Not for user instantiation
 */
@SerialVersionUID(1L)
final case class Address private[akka] (protocol: String, system: String, host: Option[String], port: Option[Int]) {
  // Please note that local/non-local distinction must be preserved:
  // host.isDefined == hasGlobalScope
  // host.isEmpty == hasLocalScope
  // hasLocalScope == !hasGlobalScope

  def this(protocol: String, system: String) = this(protocol, system, None, None)
  def this(protocol: String, system: String, host: String, port: Int) = this(protocol, system, Option(host), Some(port))

  def copy(
      protocol: String = protocol,
      system: String = system,
      host: Option[String] = host,
      port: Option[Int] = port) = {
    Address(protocol, system, host, port)
  }

  /**
   * Java API: The hostname if specified or empty optional if not
   */
  def getHost(): Optional[String] = host.toJava

  /**
   * Java API: The port if specified or empty optional if not
   */
  def getPort(): Optional[Integer] = port.toJava.asInstanceOf[Optional[Integer]]

  /**
   * Returns true if this Address is only defined locally. It is not safe to send locally scoped addresses to remote
   * hosts. See also [[akka.actor.Address#hasGlobalScope]].
   */
  def hasLocalScope: Boolean = host.isEmpty

  /**
   * Returns true if this Address is usable globally. Unlike locally defined addresses ([[akka.actor.Address#hasLocalScope]])
   * addresses of global scope are safe to sent to other hosts, as they globally and uniquely identify an addressable
   * entity.
   */
  def hasGlobalScope: Boolean = host.isDefined
  // store hashCode
  @nowarn("msg=deprecated")
  @transient override lazy val hashCode: Int = scala.util.hashing.MurmurHash3.productHash(this)

  /**
   * Returns the canonical String representation of this Address formatted as:
   *
   * `protocol://system@host:port`
   */
  @transient
  override lazy val toString: String = {
    val sb = (new java.lang.StringBuilder(protocol)).append("://").append(system)

    if (host.isDefined) sb.append('@').append(host.get)
    if (port.isDefined) sb.append(':').append(port.get)

    sb.toString
  }

  /**
   * Returns a String representation formatted as:
   *
   * `system@host:port`
   */
  def hostPort: String = toString.substring(protocol.length + 3)

  /** INTERNAL API
   * Check if the address is not created through `AddressFromURIString`, if there
   * are any unusual characters in the host string.
   */
  @InternalApi
  private[akka] def hasInvalidHostCharacters: Boolean =
    host.exists(Address.InvalidHostRegex.findFirstIn(_).nonEmpty)

  /** INTERNAL API */
  @InternalApi
  private[akka] def checkHostCharacters(): Unit =
    require(!hasInvalidHostCharacters, s"Using invalid host characters '$host' in the Address is not allowed.")

}

object Address {

  // if underscore and no dot after, then invalid
  val InvalidHostRegex = "_[^.]*$".r

  /**
   * Constructs a new Address with the specified protocol and system name
   */
  def apply(protocol: String, system: String) = new Address(protocol, system)

  /**
   * Constructs a new Address with the specified protocol, system name, host and port
   */
  def apply(protocol: String, system: String, host: String, port: Int) =
    new Address(protocol, system, Some(host), Some(port))

  /**
   * `Address` ordering type class, sorts addresses by protocol, name, host and port.
   */
  implicit val addressOrdering: Ordering[Address] = Ordering.fromLessThan[Address] { (a, b) =>
    if (a eq b) false
    else if (a.protocol != b.protocol) a.system.compareTo(b.protocol) < 0
    else if (a.system != b.system) a.system.compareTo(b.system) < 0
    else if (a.host != b.host) a.host.getOrElse("").compareTo(b.host.getOrElse("")) < 0
    else if (a.port != b.port) a.port.getOrElse(0) < b.port.getOrElse(0)
    else false
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] trait PathUtils {
  protected def split(s: String, fragment: String): List[String] = {
    @tailrec
    def rec(pos: Int, acc: List[String]): List[String] = {
      val from = s.lastIndexOf('/', pos - 1)
      val sub = s.substring(from + 1, pos)
      val l =
        if ((fragment ne null) && acc.isEmpty) sub + "#" + fragment :: acc
        else sub :: acc
      if (from == -1) l else rec(from, l)
    }
    rec(s.length, Nil)
  }
}

/**
 * Extractor for so-called “relative actor paths” as in “relative URI”, not in
 * “relative to some actor”. Examples:
 *
 *  * "grand/child"
 *  * "/user/hello/world"
 */
object RelativeActorPath extends PathUtils {
  def unapply(addr: String): Option[immutable.Seq[String]] = {
    try {
      val uri = new URI(addr)
      if (uri.isAbsolute) None
      else Some(split(uri.getRawPath, uri.getRawFragment))
    } catch {
      case _: URISyntaxException => None
    }
  }
}

/**
 * This object serves as extractor for Scala and as address parser for Java.
 */
object AddressFromURIString {
  def unapply(addr: String): Option[Address] =
    try unapply(new URI(addr))
    catch { case _: URISyntaxException => None }

  def unapply(uri: URI): Option[Address] =
    if (uri eq null) None
    else if (uri.getScheme == null || (uri.getUserInfo == null && uri.getHost == null)) None
    else if (uri.getUserInfo == null) { // case 1: “akka://system”
      if (uri.getPort != -1) None
      else Some(Address(uri.getScheme, uri.getHost))
    } else { // case 2: “akka://system@host:port”
      if (uri.getHost == null || uri.getPort == -1) None
      else
        Some(
          if (uri.getUserInfo == null) Address(uri.getScheme, uri.getHost)
          else Address(uri.getScheme, uri.getUserInfo, uri.getHost, uri.getPort))
    }

  /**
   * Try to construct an Address from the given String or throw a java.net.MalformedURLException.
   */
  def apply(addr: String): Address = addr match {
    case AddressFromURIString(address) => address
    case _                             => throw new MalformedURLException(addr)
  }

  /**
   * Java API: Try to construct an Address from the given String or throw a java.net.MalformedURLException.
   */
  def parse(addr: String): Address = apply(addr)
}

/**
 * Given an ActorPath it returns the Address and the path elements if the path is well-formed
 */
object ActorPathExtractor extends PathUtils {
  def unapply(addr: String): Option[(Address, immutable.Iterable[String])] =
    try {
      val uri = new URI(addr)
      uri.getRawPath match {
        case null => None
        case path => AddressFromURIString.unapply(uri).map((_, split(path, uri.getRawFragment).drop(1)))
      }
    } catch {
      case _: URISyntaxException => None
    }
}
