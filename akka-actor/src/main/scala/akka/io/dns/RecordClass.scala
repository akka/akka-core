/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns

final case class RecordClass(code: Short, name: String)

object RecordClass {

  val IN = RecordClass(1, "IN")
  val CS = RecordClass(2, "CS")
  val CH = RecordClass(3, "CH")
  val HS = RecordClass(4, "HS")

  val WILDCARD = RecordClass(255, "WILDCARD")

}
