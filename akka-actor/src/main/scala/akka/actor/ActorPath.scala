/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor
import java.lang.{ StringBuilder => JStringBuilder }
import java.net.MalformedURLException

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.immutable
import scala.jdk.CollectionConverters._

import akka.japi.Util.immutableSeq

/**
 * Java API
 */
object ActorPaths {
  // static forwarders to `object ActorPath`, since `trait ActorPath`
  // could not be changed to `abstract ActorPath` in a binary compatible way

  /**
   * Parse string as actor path; throws java.net.MalformedURLException if unable to do so.
   */
  def fromString(s: String): ActorPath = ActorPath.fromString(s)

  /**
   * Validates the given actor path element and throws an [[InvalidActorNameException]] if invalid.
   * See [[#isValidPathElement]] for a non-throwing version.
   *
   * @param element actor path element to be validated
   */
  final def validatePathElement(element: String): Unit = ActorPath.validatePathElement(element)

  /**
   * Validates the given actor path element and throws an [[InvalidActorNameException]] if invalid.
   * See [[#isValidPathElement]] for a non-throwing version.
   *
   * @param element actor path element to be validated
   * @param fullPath optional fullPath element that may be included for better error messages; null if not given
   */
  final def validatePathElement(element: String, fullPath: String): Unit =
    ActorPath.validatePathElement(element, fullPath)

  /**
   * This method is used to validate a path element (Actor Name).
   * Since Actors form a tree, it is addressable using an URL, therefore an Actor Name has to conform to:
   * <a href="https://www.ietf.org/rfc/rfc2396.txt">RFC-2396</a>.
   *
   * User defined Actor names may not start from a `$` sign - these are reserved for system names.
   */
  final def isValidPathElement(s: String): Boolean = ActorPath.isValidPathElement(s)

}

object ActorPath {

  /**
   * Parse string as actor path; throws java.net.MalformedURLException if unable to do so.
   */
  def fromString(s: String): ActorPath = s match {
    case ActorPathExtractor(address, elems) => RootActorPath(address) / elems
    case _                                  => throw new MalformedURLException("cannot parse as ActorPath: " + s)
  }

  private final val ValidSymbols = """-_.*$+:@&=,!~';"""

  private final val ValidPathCode = -1
  private final val EmptyPathCode = -2

  /**
   * Validates the given actor path element and throws an [[InvalidActorNameException]] if invalid.
   * See [[#isValidPathElement]] for a non-throwing version.
   *
   * @param element actor path element to be validated
   */
  final def validatePathElement(element: String): Unit = validatePathElement(element, fullPath = null)

  /**
   * Validates the given actor path element and throws an [[InvalidActorNameException]] if invalid.
   * See [[#isValidPathElement]] for a non-throwing version.
   *
   * @param element actor path element to be validated
   * @param fullPath optional fullPath element that may be included for better error messages; null if not given
   */
  final def validatePathElement(element: String, fullPath: String): Unit = {
    def fullPathMsg = if (fullPath ne null) s""" (in path [$fullPath])""" else ""

    // If the number of cases increase remember to add a `@switch` annotation e.g.:
    // (findInvalidPathElementCharPosition(element): @switch) match {
    (findInvalidPathElementCharPosition(element)) match {
      case ValidPathCode =>
      // valid
      case EmptyPathCode =>
        throw InvalidActorNameException(s"Actor path element must not be empty $fullPathMsg")
      case invalidAt =>
        throw InvalidActorNameException(s"""Invalid actor path element [$element]$fullPathMsg, illegal character [${element(
          invalidAt)}] at position: $invalidAt. """ +
        """Actor paths MUST: """ +
        """not start with `$`, """ +
        s"""include only ASCII letters and can only contain these special characters: ${ActorPath.ValidSymbols}.""")
    }
  }

  /**
   * This method is used to validate a path element (Actor Name).
   * Since Actors form a tree, it is addressable using an URL, therefore an Actor Name has to conform to:
   * <a href="https://www.ietf.org/rfc/rfc2396.txt">RFC-2396</a>.
   *
   * User defined Actor names may not start from a `$` sign - these are reserved for system names.
   */
  final def isValidPathElement(s: String): Boolean =
    findInvalidPathElementCharPosition(s) == ValidPathCode

  private final def findInvalidPathElementCharPosition(s: String): Int =
    if (s.isEmpty) EmptyPathCode
    else {
      def isValidChar(c: Char): Boolean =
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (ValidSymbols.indexOf(c) != -1)

      def isHexChar(c: Char): Boolean =
        (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') || (c >= '0' && c <= '9')

      val len = s.length
      def validate(pos: Int): Int =
        if (pos < len)
          s.charAt(pos) match {
            case c if isValidChar(c) => validate(pos + 1)
            case '%' if pos + 2 < len && isHexChar(s.charAt(pos + 1)) && isHexChar(s.charAt(pos + 2)) =>
              validate(pos + 3)
            case _ => pos
          } else ValidPathCode

      if (len > 0 && s.charAt(0) != '$') validate(0) else 0
    }

  private[akka] final val emptyActorPath: immutable.Iterable[String] = List("")
}

/**
 * Actor path is a unique path to an actor that shows the creation path
 * up through the actor tree to the root actor.
 *
 * ActorPath defines a natural ordering (so that ActorRefs can be put into
 * collections with this requirement); this ordering is intended to be as fast
 * as possible, which owing to the bottom-up recursive nature of ActorPath
 * is sorted by path elements FROM RIGHT TO LEFT, where RootActorPath &gt;
 * ChildActorPath in case the number of elements is different.
 *
 * Two actor paths are compared equal when they have the same name and parent
 * elements, including the root address information. That does not necessarily
 * mean that they point to the same incarnation of the actor if the actor is
 * re-created with the same path. In other words, in contrast to how actor
 * references are compared the unique id of the actor is not taken into account
 * when comparing actor paths.
 */
@nowarn("msg=@SerialVersionUID has no effect on traits")
@SerialVersionUID(1L)
sealed trait ActorPath extends Comparable[ActorPath] with Serializable {

  /**
   * The Address under which this path can be reached; walks up the tree to
   * the RootActorPath.
   */
  def address: Address

  /**
   * The name of the actor that this path refers to.
   */
  def name: String

  /**
   * The path for the parent actor.
   */
  def parent: ActorPath

  /**
   * Create a new child actor path.
   */
  def /(child: String): ActorPath

  /**
   * Java API: Create a new child actor path.
   */
  def child(child: String): ActorPath = /(child)

  /**
   * Recursively create a descendant’s path by appending all child names.
   */
  def /(child: Iterable[String]): ActorPath =
    child.foldLeft(this)((path, elem) => if (elem.isEmpty) path else path / elem)

  /**
   * Java API: Recursively create a descendant’s path by appending all child names.
   */
  def descendant(names: java.lang.Iterable[String]): ActorPath = /(immutableSeq(names))

  /**
   * Sequence of names for this path from root to this. Performance implication: has to allocate a list.
   */
  def elements: immutable.Iterable[String]

  /**
   * Java API: Sequence of names for this path from root to this. Performance implication: has to allocate a list.
   */
  def getElements: java.lang.Iterable[String] = elements.asJava

  /**
   * Walk up the tree to obtain and return the RootActorPath.
   */
  def root: RootActorPath

  /**
   * String representation of the path elements, excluding the address
   * information. The elements are separated with "/" and starts with "/",
   * e.g. "/user/a/b".
   */
  def toStringWithoutAddress: String = elements.mkString("/", "/", "")

  /**
   * Generate String representation, replacing the Address in the RootActor
   * Path with the given one unless this path’s address includes host and port
   * information.
   */
  def toStringWithAddress(address: Address): String

  /**
   * Generate full String representation including the
   * uid for the actor cell instance as URI fragment.
   * This representation should be used as serialized
   * representation instead of `toString`.
   */
  def toSerializationFormat: String

  /**
   * Generate full String representation including the uid for the actor cell
   * instance as URI fragment, replacing the Address in the RootActor Path
   * with the given one unless this path’s address includes host and port
   * information. This representation should be used as serialized
   * representation instead of `toStringWithAddress`.
   */
  def toSerializationFormatWithAddress(address: Address): String

  /**
   * INTERNAL API
   * Unique identifier of the actor. Used for distinguishing
   * different incarnations of actors with same path (name elements).
   */
  private[akka] def uid: Int

  /**
   * INTERNAL API
   * Creates a new ActorPath with same elements but with the specified `uid`.
   */
  private[akka] def withUid(uid: Int): ActorPath

}

/**
 * Root of the hierarchy of ActorPaths. There is exactly root per ActorSystem
 * and node (for remote-enabled or clustered systems).
 */
@SerialVersionUID(1L)
final case class RootActorPath(address: Address, name: String = "/") extends ActorPath {
  require(
    name.length == 1 || name.indexOf('/', 1) == -1,
    ("/ may only exist at the beginning of the root actors name, " +
    "it is a path separator and is not legal in ActorPath names: [%s]").format(name))
  require(name.indexOf('#') == -1, "# is a fragment separator and is not legal in ActorPath names: [%s]".format(name))

  override def parent: ActorPath = this

  override def root: RootActorPath = this

  override def /(child: String): ActorPath = {
    val (childName, uid) = ActorCell.splitNameAndUid(child)
    new ChildActorPath(this, childName, uid)
  }

  override def elements: immutable.Iterable[String] = ActorPath.emptyActorPath

  override val toString: String = address.toString + name

  override val toSerializationFormat: String = toString

  override def toStringWithAddress(addr: Address): String =
    if (address.host.isDefined) address.toString + name
    else addr.toString + name

  override def toSerializationFormatWithAddress(addr: Address): String = toStringWithAddress(addr)

  override def compareTo(other: ActorPath): Int = other match {
    case r: RootActorPath =>
      toString.compareTo(r.toString) // FIXME make this cheaper by comparing address and name in isolation
    case _: ChildActorPath => 1
  }

  /**
   * INTERNAL API
   */
  private[akka] def uid: Int = ActorCell.undefinedUid

  /**
   * INTERNAL API
   */
  override private[akka] def withUid(uid: Int): ActorPath =
    if (uid == ActorCell.undefinedUid) this
    else throw new IllegalStateException(s"RootActorPath must have undefinedUid, [$uid != ${ActorCell.undefinedUid}")

}

/**
 * Not for user instantiation
 */
@SerialVersionUID(1L)
final class ChildActorPath private[akka] (val parent: ActorPath, val name: String, override private[akka] val uid: Int)
    extends ActorPath {
  if (name.indexOf('/') != -1)
    throw new IllegalArgumentException("/ is a path separator and is not legal in ActorPath names: [%s]".format(name))
  if (name.indexOf('#') != -1)
    throw new IllegalArgumentException(
      "# is a fragment separator and is not legal in ActorPath names: [%s]".format(name))

  override def address: Address = root.address

  override def /(child: String): ActorPath = {
    val (childName, uid) = ActorCell.splitNameAndUid(child)
    new ChildActorPath(this, childName, uid)
  }

  override def elements: immutable.Iterable[String] = {
    @tailrec
    def rec(p: ActorPath, acc: List[String]): immutable.Iterable[String] = p match {
      case _: RootActorPath => acc
      case _                => rec(p.parent, p.name :: acc)
    }
    rec(this, Nil)
  }

  override def root: RootActorPath = {
    @tailrec
    def rec(p: ActorPath): RootActorPath = p match {
      case r: RootActorPath => r
      case _                => rec(p.parent)
    }
    rec(this)
  }

  /**
   * INTERNAL API
   */
  override private[akka] def withUid(uid: Int): ActorPath =
    if (uid == this.uid) this
    else new ChildActorPath(parent, name, uid)

  override def toString: String = {
    val length = toStringLength
    buildToString(new JStringBuilder(length), length, 0, _.toString).toString
  }

  override def toSerializationFormat: String = {
    val length = toStringLength
    val sb = buildToString(new JStringBuilder(length + 12), length, 0, _.toString)
    appendUidFragment(sb).toString
  }

  private def toStringLength: Int = toStringOffset + name.length

  private val toStringOffset: Int = parent match {
    case r: RootActorPath  => r.address.toString.length + r.name.length
    case c: ChildActorPath => c.toStringLength + 1
  }

  override def toStringWithAddress(addr: Address): String = {
    if (IgnoreActorRef.isIgnoreRefPath(this)) {
      // we never change address for IgnoreActorRef
      this.toString
    } else {
      val diff = addressStringLengthDiff(addr)
      val length = toStringLength + diff
      buildToString(new JStringBuilder(length), length, diff, _.toStringWithAddress(addr)).toString
    }
  }

  override def toSerializationFormatWithAddress(addr: Address): String = {
    if (IgnoreActorRef.isIgnoreRefPath(this)) {
      // we never change address for IgnoreActorRef
      this.toString
    } else {
      val diff = addressStringLengthDiff(addr)
      val length = toStringLength + diff
      val sb = buildToString(new JStringBuilder(length + 12), length, diff, _.toStringWithAddress(addr))
      appendUidFragment(sb).toString
    }
  }

  private def addressStringLengthDiff(address: Address): Int = {
    val r = root
    if (r.address.host.isDefined) 0
    else address.toString.length - r.address.toString.length
  }

  /**
   * Optimized toString construction. Used by `toString`, `toSerializationFormat`,
   * and friends `WithAddress`
   * @param sb builder that will be modified (and same instance is returned)
   * @param length pre-calculated length of the to be constructed String, not
   *   necessarily same as sb.capacity because more things may be appended to the
   *   sb afterwards
   * @param diff difference in offset for each child element, due to different address
   * @param rootString function to construct the root element string
   */
  private def buildToString(
      sb: JStringBuilder,
      length: Int,
      diff: Int,
      rootString: RootActorPath => String): JStringBuilder = {
    @tailrec
    def rec(p: ActorPath): JStringBuilder = p match {
      case r: RootActorPath =>
        val rootStr = rootString(r)
        sb.replace(0, rootStr.length, rootStr)
      case c: ChildActorPath =>
        val start = c.toStringOffset + diff
        val end = start + c.name.length
        sb.replace(start, end, c.name)
        if (c ne this)
          sb.replace(end, end + 1, "/")
        rec(c.parent)
    }

    sb.setLength(length)
    rec(this)
  }

  private def appendUidFragment(sb: JStringBuilder): JStringBuilder = {
    if (uid == ActorCell.undefinedUid) sb
    else sb.append("#").append(uid)
  }

  override def equals(other: Any): Boolean = {
    @tailrec
    def rec(left: ActorPath, right: ActorPath): Boolean =
      if (left eq right) true
      else if (left.isInstanceOf[RootActorPath]) left.equals(right)
      else if (right.isInstanceOf[RootActorPath]) right.equals(left)
      else left.name == right.name && rec(left.parent, right.parent)

    other match {
      case p: ActorPath => rec(this, p)
      case _            => false
    }
  }

  // TODO RK investigate Phil’s hash from scala.collection.mutable.HashTable.improve
  override def hashCode: Int = {
    import akka.routing.MurmurHash._

    @tailrec
    def rec(p: ActorPath, h: Int, c: Int, k: Int): Int = p match {
      case r: RootActorPath => extendHash(h, r.##, c, k)
      case _                => rec(p.parent, extendHash(h, stringHash(name), c, k), nextMagicA(c), nextMagicB(k))
    }

    finalizeHash(rec(this, startHash(42), startMagicA, startMagicB))
  }

  override def compareTo(other: ActorPath): Int = {
    @tailrec
    def rec(left: ActorPath, right: ActorPath): Int =
      if (left eq right) 0
      else if (left.isInstanceOf[RootActorPath]) left.compareTo(right)
      else if (right.isInstanceOf[RootActorPath]) -right.compareTo(left)
      else {
        val x = left.name.compareTo(right.name)
        if (x == 0) rec(left.parent, right.parent)
        else x
      }

    rec(this, other)
  }
}
