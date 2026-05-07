/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics

import java.io.File

import scala.language.postfixOps
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import kamon.sigar.SigarProvisioner
import org.hyperic.sigar.Sigar
import org.hyperic.sigar.SigarProxy

/**
 * Provide sigar instance as `SigarProxy`.
 *
 * User can provision sigar classes and native library in one of the following ways:
 *
 * 1) Use <a href="https://github.com/kamon-io/sigar-loader">Kamon sigar-loader</a> as a project dependency for the user project.
 * Metrics extension will extract and load sigar library on demand with help of Kamon sigar provisioner.
 *
 * 2) Use <a href="https://github.com/kamon-io/sigar-loader">Kamon sigar-loader</a> as java agent: `java -javaagent:/path/to/sigar-loader.jar`
 * Kamon sigar loader agent will extract and load sigar library during JVM start.
 *
 * 3) Place `sigar.jar` on the `classpath` and sigar native library for the o/s on the `java.library.path`
 * User is required to manage both project dependency and library deployment manually.
 */
trait SigarProvider {

  /** Library extract location. */
  def extractFolder: String

  /** Verify if sigar native library is loaded and operational. */
  def isNativeLoaded: Boolean =
    try {
      val sigar = verifiedSigarInstance
      SigarProvider.close(sigar)
      true
    } catch {
      case _: Throwable => false
    }

  /** Create sigar and verify it works. */
  def verifiedSigarInstance: SigarProxy = {
    val sigar = new Sigar()
    sigar.getPid
    sigar.getLoadAverage
    sigar.getCpuPerc
    sigar
  }

  /** Extract and load sigar native library. */
  def provisionSigarLibrary(): Unit = {
    SigarProvisioner.provision(new File(extractFolder))
  }

  /**
   *  Create sigar instance with 2-phase sigar library loading.
   *  1) Assume that library is already provisioned.
   *  2) Attempt to provision library via sigar-loader.
   */
  def createSigarInstance: SigarProxy = {
    TryNative {
      verifiedSigarInstance
    }.orElse(TryNative {
        provisionSigarLibrary()
        verifiedSigarInstance
      })
      .recover {
        case e: Throwable => throw new RuntimeException("Failed to load sigar:", e)
      } get
  }

}

object SigarProvider {

  /**
   * Release underlying sigar proxy resources.
   *
   * Note: `SigarProxy` is not `Sigar` during tests.
   */
  def close(sigar: SigarProxy) = {
    if (sigar.isInstanceOf[Sigar]) sigar.asInstanceOf[Sigar].close()
  }
}

/**
 * Provide sigar instance as `SigarProxy` with configured location via [[ClusterMetricsSettings]].
 */
case class DefaultSigarProvider(settings: ClusterMetricsSettings) extends SigarProvider {
  def extractFolder = settings.NativeLibraryExtractFolder
}

/**
 * INTERNAL API
 */
private[metrics] object TryNative {
  def apply[T](r: => T): Try[T] =
    try Success(r)
    catch {
      // catching all, for example java.lang.LinkageError that are not caught by `NonFatal` in `Try`
      case e: Throwable => Failure(e)
    }
}
