/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.lang.reflect.InvocationTargetException

import scala.collection.immutable
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Try

import akka.annotation.DoNotInherit

/**
 * This is the default [[akka.actor.DynamicAccess]] implementation used by [[akka.actor.ExtendedActorSystem]]
 * unless overridden. It uses reflection to turn fully-qualified class names into `Class[_]` objects
 * and creates instances from there using `getDeclaredConstructor()` and invoking that. The class loader
 * to be used for all this is determined by the actor system’s class loader by default.
 *
 * Not for user extension or construction
 */
@DoNotInherit
class ReflectiveDynamicAccess(val classLoader: ClassLoader) extends DynamicAccess {

  override def getClassFor[T: ClassTag](fqcn: String): Try[Class[_ <: T]] =
    Try[Class[_ <: T]]({
      val c = Class.forName(fqcn, false, classLoader).asInstanceOf[Class[_ <: T]]
      val t = implicitly[ClassTag[T]].runtimeClass
      if (t.isAssignableFrom(c)) c else throw new ClassCastException(t.toString + " is not assignable from " + c)
    })

  override def createInstanceFor[T: ClassTag](clazz: Class[_], args: immutable.Seq[(Class[_], AnyRef)]): Try[T] =
    Try {
      val types = args.map(_._1).toArray
      val values = args.map(_._2).toArray
      val constructor = clazz.getDeclaredConstructor(types: _*)
      constructor.setAccessible(true)
      val obj = constructor.newInstance(values: _*)
      val t = implicitly[ClassTag[T]].runtimeClass
      if (t.isInstance(obj)) obj.asInstanceOf[T]
      else throw new ClassCastException(clazz.getName + " is not a subtype of " + t)
    }.recover { case i: InvocationTargetException if i.getTargetException ne null => throw i.getTargetException }

  override def createInstanceFor[T: ClassTag](fqcn: String, args: immutable.Seq[(Class[_], AnyRef)]): Try[T] =
    getClassFor(fqcn).flatMap { c =>
      createInstanceFor(c, args)
    }

  override def classIsOnClasspath(fqcn: String): Boolean =
    getClassFor[Any](fqcn) match {
      case Failure(_: ClassNotFoundException | _: NoClassDefFoundError) =>
        false
      case _ =>
        true
    }

  override def getObjectFor[T: ClassTag](fqcn: String): Try[T] = {
    val classTry =
      if (fqcn.endsWith("$")) getClassFor(fqcn)
      else getClassFor(fqcn + "$").recoverWith { case _ => getClassFor(fqcn) }
    classTry.flatMap { c =>
      Try {
        val module = c.getDeclaredField("MODULE$")
        module.setAccessible(true)
        val t = implicitly[ClassTag[T]].runtimeClass
        module.get(null) match {
          case null                  => throw new NullPointerException
          case x if !t.isInstance(x) => throw new ClassCastException(fqcn + " is not a subtype of " + t)
          case x: T                  => x
          case unexpected =>
            throw new IllegalArgumentException(s"Unexpected module field: $unexpected") // will not happen, for exhaustiveness check
        }
      }.recover { case i: InvocationTargetException if i.getTargetException ne null => throw i.getTargetException }
    }
  }
}
