/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp.ssl

import java.security.cert.X509Certificate
import java.util
import javax.naming.ldap.LdapName

import akka.annotation.InternalApi
import scala.jdk.CollectionConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object X509Readers {

  def getAllSubjectNames(cert: X509Certificate): Set[String] = {
    val maybeCommonName =
      new LdapName(cert.getSubjectX500Principal.getName).getRdns.asScala.collectFirst {
        case attr if attr.getType.equalsIgnoreCase("CN") =>
          attr.getValue.toString
      }

    val iterable: Iterable[util.List[_]] = Option(cert.getSubjectAlternativeNames).map(_.asScala).getOrElse(Nil)
    val alternates = iterable.collect {
      // See the javadocs of cert.getSubjectAlternativeNames for what this list contains,
      // first element should be an integer, if that integer is 2, then the second element
      // is a String containing the DNS name.
      case list if list.size() == 2 && list.get(0) == 2 =>
        list.get(1) match {
          case dnsName: String => dnsName
          case other =>
            throw new IllegalArgumentException(
              s"Error reading Subject Alternative Name, expected dns name to be a String, but instead got a ${other.getClass}")
        }
    }

    maybeCommonName.toSet ++ alternates
  }

}
