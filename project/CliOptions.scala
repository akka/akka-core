/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

case class CliOption[T](private val value: T) {
  def get: T = value
}

object CliOption {
  def apply[T](path: String, default: T)(implicit ev: CliOptionParser[T]): CliOption[T] = ev.parse(path, default)

  implicit class BooleanCliOption(cliOption: CliOption[Boolean]) {
    def ifTrue[A](a: => A): Option[A] = if (cliOption.get) Some(a) else None
  }

  trait CliOptionParser[T] {
    def parse(path: String, default: T): CliOption[T]
  }

  object CliOptionParser {
    implicit object BooleanCliOptionParser extends CliOptionParser[Boolean] {
      def parse(path: String, default: Boolean) =
        CliOption(sys.props.getOrElse(path, default.toString).toBoolean)
    }
  }
}
