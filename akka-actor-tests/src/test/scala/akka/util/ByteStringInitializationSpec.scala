/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.io.InputStream

import scala.collection.mutable
import scala.collection.mutable.ArrayBuilder

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ByteStringInitializationSpec extends AnyWordSpec with Matchers {
  "ByteString intialization" should {
    "not get confused by initializing CompactByteString before ByteString" in {
      // a classloader that creates a new universe of classes for everything beneath akka
      // that prevents that this test interacts with any tests
      val cleanCl = new ClassLoader(null) {
        val outerCl = ByteStringInitializationSpec.this.getClass.getClassLoader
        val buffer = new Array[Byte](1000000)
        override def loadClass(name: String): Class[_] =
          if (!name.startsWith("akka")) outerCl.loadClass(name)
          else {
            val classFile = name.replace(".", "/") + ".class"
            val is = outerCl.getResourceAsStream(classFile)
            val res = slurp(is, new mutable.ArrayBuilder.ofByte)
            defineClass(name, res, 0, res.length)
          }

        def slurp(is: InputStream, res: ArrayBuilder[Byte]): Array[Byte] = {
          val read = is.read(buffer)
          if (read == 0) throw new IllegalStateException
          else if (read > 0) slurp(is, res ++= buffer.take(read))
          else res.result()
        }
      }

      cleanCl
        .loadClass("akka.util.ByteStringInitTest")
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[Runnable]
        .run()
    }
  }
}

class ByteStringInitTest extends Runnable {
  def run(): Unit = {
    require(CompactByteString.empty ne null)
    require(ByteString.empty ne null)
  }
}
