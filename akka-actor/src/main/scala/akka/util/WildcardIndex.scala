/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.annotation.tailrec
import scala.collection.immutable.HashMap

private[akka] final case class WildcardIndex[T](
    wildcardTree: WildcardTree[T] = WildcardTree[T](),
    doubleWildcardTree: WildcardTree[T] = WildcardTree[T]()) {

  def insert(elems: Array[String], d: T): WildcardIndex[T] = elems.lastOption match {
    case Some("**") => copy(doubleWildcardTree = doubleWildcardTree.insert(elems.iterator, d))
    case Some(_)    => copy(wildcardTree = wildcardTree.insert(elems.iterator, d))
    case _          => this
  }

  def find(elems: Iterable[String]): Option[T] =
    (if (wildcardTree.isEmpty) {
       if (doubleWildcardTree.isEmpty) {
         WildcardTree[T]() // empty
       } else {
         doubleWildcardTree.findWithTerminalDoubleWildcard(elems.iterator)
       }
     } else {
       val withSingleWildcard = wildcardTree.findWithSingleWildcard(elems.iterator)
       if (withSingleWildcard.isEmpty) {
         doubleWildcardTree.findWithTerminalDoubleWildcard(elems.iterator)
       } else {
         withSingleWildcard
       }
     }).data

  def isEmpty: Boolean = wildcardTree.isEmpty && doubleWildcardTree.isEmpty

}

private[akka] object WildcardTree {
  private val empty = new WildcardTree[Nothing]()
  def apply[T](): WildcardTree[T] = empty.asInstanceOf[WildcardTree[T]]
}

private[akka] final case class WildcardTree[T](
    data: Option[T] = None,
    children: Map[String, WildcardTree[T]] = HashMap[String, WildcardTree[T]](),
    wildcardSuffixChildren: Map[String, WildcardTree[T]] = HashMap[String, WildcardTree[T]]()) {

  def isEmpty: Boolean = data.isEmpty && children.isEmpty

  def insert(elems: Iterator[String], d: T): WildcardTree[T] =
    if (!elems.hasNext) {
      copy(data = Some(d))
    } else {
      val e = elems.next()
      if (e != "**" && e.endsWith("**"))
        throw new IllegalArgumentException(
          "double wildcard can't be used as a suffix (e.g. /user/actor**), only as a full subPath element (e.g. /user/actor/**)")
      else if (e != "*" && e != "**" && e.endsWith("*"))
        copy(
          wildcardSuffixChildren = wildcardSuffixChildren
            .updated(e.stripSuffix("*"), wildcardSuffixChildren.getOrElse(e, WildcardTree[T]()).insert(elems, d)))
      else
        copy(children = children.updated(e, children.getOrElse(e, WildcardTree[T]()).insert(elems, d)))
    }

  @tailrec def findWithSingleWildcard(elems: Iterator[String]): WildcardTree[T] =
    if (!elems.hasNext) this
    else {
      val nextElement = elems.next()
      children.get(nextElement) match {
        case Some(branch) => branch.findWithSingleWildcard(elems)
        case None =>
          children.get("*") match {
            case Some(branch) => branch.findWithSingleWildcard(elems)
            case None =>
              val maybeWildcardSuffixMatchingBranch = wildcardSuffixChildren.collectFirst {
                case (key, branch) if nextElement.startsWith(key) => branch
              }
              maybeWildcardSuffixMatchingBranch match {
                case Some(branch) => branch.findWithSingleWildcard(elems)
                case None         => WildcardTree[T]()
              }
          }
      }
    }

  @tailrec def findWithTerminalDoubleWildcard(
      elems: Iterator[String],
      alt: WildcardTree[T] = WildcardTree[T]()): WildcardTree[T] = {
    if (!elems.hasNext) this
    else {
      val newAlt = children.getOrElse("**", alt)
      children.get(elems.next()) match {
        case Some(branch) => branch.findWithTerminalDoubleWildcard(elems, newAlt)
        case None =>
          children.get("*") match {
            case Some(branch) => branch.findWithTerminalDoubleWildcard(elems, newAlt)
            case None         => newAlt
          }
      }
    }
  }
}
