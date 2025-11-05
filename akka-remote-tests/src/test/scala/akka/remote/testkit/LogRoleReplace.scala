/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.testkit

import java.awt.Toolkit
import java.awt.datatransfer.Clipboard
import java.awt.datatransfer.ClipboardOwner
import java.awt.datatransfer.DataFlavor
import java.awt.datatransfer.StringSelection
import java.awt.datatransfer.Transferable
import java.io.BufferedReader
import java.io.FileReader
import java.io.FileWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.io.StringReader
import java.io.StringWriter

import scala.annotation.tailrec

/**
 * Utility to make log files from multi-node tests easier to analyze.
 * Replaces jvm names and host:port with corresponding logical role name.
 */
object LogRoleReplace extends ClipboardOwner {

  /**
   * Main program. Use with 0, 1 or 2 arguments.
   *
   * When using 0 arguments it reads from standard input
   * (System.in) and writes to standard output (System.out).
   *
   * With 1 argument it reads from the file specified in the first argument
   * and writes to standard output.
   *
   * With 2 arguments it reads the file specified in the first argument
   * and writes to the file specified in the second argument.
   *
   * You can also replace the contents of the clipboard instead of using files
   * by supplying `clipboard` as argument
   */
  def main(args: Array[String]): Unit = {
    val replacer = new LogRoleReplace

    if (args.length == 0) {
      replacer.process(
        new BufferedReader(new InputStreamReader(System.in)),
        new PrintWriter(new OutputStreamWriter(System.out)))

    } else if (args(0) == "clipboard") {
      val clipboard = Toolkit.getDefaultToolkit.getSystemClipboard
      val contents = clipboard.getContents(null)
      if (contents != null && contents.isDataFlavorSupported(DataFlavor.stringFlavor)) {
        val text = contents.getTransferData(DataFlavor.stringFlavor).asInstanceOf[String]
        val result = new StringWriter
        replacer.process(new BufferedReader(new StringReader(text)), new PrintWriter(result))
        clipboard.setContents(new StringSelection(result.toString), this)
        println("Replaced clipboard contents")
      }

    } else if (args.length == 1) {
      val inputFile = new BufferedReader(new FileReader(args(0)))
      try {
        replacer.process(inputFile, new PrintWriter(new OutputStreamWriter(System.out)))
      } finally {
        inputFile.close()
      }

    } else if (args.length == 2) {
      val outputFile = new PrintWriter(new FileWriter(args(1)))
      val inputFile = new BufferedReader(new FileReader(args(0)))
      try {
        replacer.process(inputFile, outputFile)
      } finally {
        outputFile.close()
        inputFile.close()
      }
    }
  }

  /**
   * Empty implementation of the ClipboardOwner interface
   */
  def lostOwnership(clipboard: Clipboard, contents: Transferable): Unit = ()
}

class LogRoleReplace {

  private val RoleStarted =
    """\[([\w\-]+)\].*Role \[([\w]+)\] started with address \[[\w\-\+\.]+://.*@([\w\-\.]+):([0-9]+)\]""".r
  private val ColorCode = "\u001B?\\[[0-9]+m"

  private var replacements: Map[String, String] = Map.empty

  def process(in: BufferedReader, out: PrintWriter): Unit = {

    @tailrec
    def processLines(line: String): Unit =
      if (line ne null) {
        out.println(processLine(line))
        processLines(in.readLine)
      }

    processLines(in.readLine())
  }

  def processLine(line: String): String = {
    val cleanLine = removeColorCodes(line)
    if (updateReplacements(cleanLine))
      replaceLine(cleanLine)
    else
      cleanLine
  }

  private def removeColorCodes(line: String): String =
    line.replaceAll(ColorCode, "")

  private def updateReplacements(line: String): Boolean = {
    if (line.startsWith("[info] * ")) {
      // reset when new test begins
      replacements = Map.empty
    }

    line match {
      case RoleStarted(jvm, role, host, port) =>
        replacements += (jvm -> role)
        replacements += ((host + ":" + port) -> role)
        false
      case _ => true
    }
  }

  private def replaceLine(line: String): String = {
    var result = line
    for ((from, to) <- replacements) {
      result = result.replaceAll(from, to)
    }
    result
  }

}
