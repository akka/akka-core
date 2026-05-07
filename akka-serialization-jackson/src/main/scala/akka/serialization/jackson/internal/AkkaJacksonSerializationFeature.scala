/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization.jackson.internal

import akka.annotation.InternalApi
import akka.serialization.jackson.CborSerializable
import akka.serialization.jackson.JsonSerializable
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import org.graalvm.nativeimage.hosted.Feature
import org.graalvm.nativeimage.hosted.RuntimeReflection
import org.graalvm.nativeimage.hosted.RuntimeResourceAccess

import java.lang.reflect.Modifier
import java.util
import java.util.stream.Collectors

/**
 * Provides auto-registration of user classes marked with the built in marker types.
 *
 * INTERNAL API
 */
@InternalApi
final class AkkaJacksonSerializationFeature extends Feature {
  // Note: Scala stdlib APIs must be used very sparsely/carefully here to not get init-at-build errors
  // (Array#isEmpty and Array#size are a no no's, for example)

  private val debug = System.getProperty("akka.native-image.debug") == "true"

  private val alreadyRegisteredType = new java.util.HashSet[String]()
  private val alreadyRegisteredClassResources = new java.util.HashSet[String]()

  override def beforeAnalysis(access: Feature.BeforeAnalysisAccess): Unit = {
    try {
      // Concrete message types (defined by users) that can be serializable must have a reflection entry and reflection
      // construction access so that Jackson can instantiate and set fields. The message classes will all
      // be used in user code, if not the message is never sent or received, so Graal will find them as reachable.
      // That makes it possible to auto-register reflection entries for all messages tagged with either of the two built in
      // marker traits.
      val jsonSerializable =
        access.findClassByName(classOf[JsonSerializable].getName)

      access.registerSubtypeReachabilityHandler({ (access, subtype) =>
        if (subtype != null) {
          registerTypeForJacksonSerialization(access, subtype)
        }
      }, jsonSerializable)

      val cborSerializable =
        access.findClassByName(classOf[CborSerializable].getName)

      access.registerSubtypeReachabilityHandler({ (access, subtype) =>
        if (subtype != null) {
          registerTypeForJacksonSerialization(access, subtype)
        }
      }, cborSerializable)

      // FIXME some overlap here with manually defined entries in NativeImageMetadataSpec, one place is probably enough?
      val jsonStdSerializer = access.findClassByName(classOf[StdSerializer[_]].getName)
      access.registerSubtypeReachabilityHandler({ (_, subtype) =>
        if (subtype != null) {
          registerCustomJacksonSerializers(subtype)
        }
      }, jsonStdSerializer)

      val jsonStdDeserializer = access.findClassByName(classOf[StdDeserializer[_]].getName)
      access.registerSubtypeReachabilityHandler({ (_, subtype) =>
        if (subtype != null) {
          registerCustomJacksonSerializers(subtype)
        }
      }, jsonStdDeserializer)

    } catch {
      case th: Throwable =>
        System.err.println("Akka Jackson Serialization feature failed")
        th.printStackTrace()
    }
  }

  private def registerTypeForJacksonSerialization(access: Feature.DuringAnalysisAccess, clazz: Class[_]): Unit = {

    if (!alreadyRegisteredType.contains(clazz.getName) && clazz.getPackage != null && !clazz.getPackage.getName
          .startsWith("java") && !clazz.getPackage.getName.startsWith("scala")) {
      alreadyRegisteredType.add(clazz.getName)
      log("Registering for jackson serialization: " + clazz.getName)
      RuntimeReflection.register(clazz)
      RuntimeReflection.registerAllDeclaredFields(clazz)
      RuntimeReflection.registerAllDeclaredMethods(clazz)

      // FIXME we could do this only if Scala class to bloat Java apps less
      registerClassResourceForAccess(access, clazz)

      try {
        val scalaModuleField = clazz.getDeclaredField("MODULE$")
        RuntimeReflection.register(scalaModuleField)
        log("Registering Scala module for " + clazz.getName)
      } catch {
        case _: NoSuchFieldException => // no scala module, this is ok
      }

      // getters
      val scalaCaseClass = try {
        clazz.getMethod("productElementNames")
        true
      } catch {
        case _: NoSuchMethodException => false
      }
      util.Arrays
        .stream(clazz.getDeclaredMethods)
        .forEach(method =>
          if (method.getParameterTypes.length == 0 && isPossiblyGetter(scalaCaseClass, method.getName)) {
            log("Registering method " + clazz.getName + "." + method.getName)
            RuntimeReflection.register(method)
            RuntimeReflection.registerAsQueried(method)
          })

      RuntimeReflection.registerAllConstructors(clazz)
      RuntimeReflection.registerAllDeclaredConstructors(clazz)
      // we still need explicit register of each constructor for some reason
      util.Arrays.stream(clazz.getDeclaredConstructors).forEach { constructor =>
        log(
          "Registering constructor " + clazz.getName + "(" + util.Arrays
            .stream(constructor.getParameters)
            .map(param => param.getName + ": " + param.getType.getName)
            .collect(Collectors.joining(", ")) + ")")
        RuntimeReflection.register(constructor)
        RuntimeReflection.registerAsQueried(constructor)
        RuntimeReflection.registerConstructorLookup(clazz, constructor.getParameterTypes: _*)
        // also register each constructor parameter type
        util.Arrays.stream(constructor.getParameterTypes).forEach { parameterType =>
          if (parameterType.getPackage != null) {
            val packageName = parameterType.getPackage.getName

            // avoid digging to deep into akka and stdlib types
            if (!packageName.startsWith("akka") && !packageName.startsWith("java") && !packageName
                  .startsWith("scala")) {
              registerTypeForJacksonSerialization(access, parameterType)

              if (parameterType.isInterface) {
                // ADT or something like it, try to register concrete classes for that interface as well
                access.registerSubtypeReachabilityHandler({ (access, clazz) =>
                  registerTypeForJacksonSerialization(access, clazz)
                }, parameterType)
              }
              if (classOf[scala.Enumeration#Value].isAssignableFrom(parameterType)) {
                warning(
                  "Saw a scala.Enumeration field. This is not supported out of the box and will require adding manual metadata")
              }
            }
          }
        }
      }

      if (!clazz.getName.endsWith("$")) {
        // check for companion object
        val companion = access.findClassByName(clazz.getName + "$")
        if (companion != null) {
          log("Registering Scala companion object for " + clazz.getName)
          RuntimeReflection.register(companion)
          RuntimeReflection.registerAllMethods(companion)

          try {
            val scalaModuleField = companion.getDeclaredField("MODULE$")
            RuntimeReflection.register(scalaModuleField)
          } catch {
            case _: NoSuchFieldException => // no scala module, this is ok
          }
        }
      }

      def registerEnclosing(clazz: Class[_]): Unit = {
        if (clazz.getEnclosingClass != null) {
          // Jackson Scala module looks at enclosing if it exists (scala.reflect.ScalaSignature may go there)
          val enclosing = clazz.getEnclosingClass
          log("Registering enclosing class/object for " + enclosing.getName)
          RuntimeReflection.register(enclosing)
          registerEnclosing(enclosing)
        }
      }
      // recursively register all enclosing classes/objects
      registerEnclosing(clazz)
    }
  }

  private def registerClassResourceForAccess(access: Feature.DuringAnalysisAccess, clazz: Class[_]): Unit = {
    if (!alreadyRegisteredClassResources.contains(clazz.getName)) {
      // Scala Jackson support needs the class files as resources as well
      alreadyRegisteredClassResources.add(clazz.getName)
      val resourceName = clazz.getName.replace('.', '/') + ".class"
      if (access.getApplicationClassLoader.getResource(resourceName) != null) {
        log("Registering class resource " + resourceName)
        RuntimeResourceAccess.addResource(clazz.getModule, resourceName)
      }
    }

  }

  private def registerCustomJacksonSerializers(subtype: Class[_]): Unit = {
    if (subtype != null && !subtype.isInterface && !Modifier.isAbstract(subtype.getModifiers) && subtype.getPackage != null && !subtype.getPackage.getName
          .startsWith("com.fasterxml.jackson") && !subtype.getPackage.getName.startsWith("akka")) {
      log("Registering custom Jackson JsonSerializer: " + subtype.getName)
      RuntimeReflection.register(subtype)
      RuntimeReflection.registerForReflectiveInstantiation(subtype)
    }
  }

  private def isPossiblyGetter(parentIsCaseClass: Boolean, methodName: String): Boolean = methodName match {
    case "toString"     => false
    case "hashCode"     => false
    case "writeReplace" => false
    // only filter if scala case class, for regular classes user may call legit fields some of these
    case "productPrefix" if parentIsCaseClass       => false
    case "productArity" if parentIsCaseClass        => false
    case "productElementNames" if parentIsCaseClass => false
    case "productIterator" if parentIsCaseClass     => false
    // scala case class copy default values
    case str if str.startsWith("copy$default") => false
    case _                                     => true
  }

  private def log(msg: String): Unit = {
    if (debug)
      System.out.println("[DEBUG] [AkkaJacksonSerializationFeature] " + msg)
  }

  private def warning(msg: String): Unit = {
    System.out.println("[WARN] [AkkaJacksonSerializationFeature] " + msg)
  }
}
