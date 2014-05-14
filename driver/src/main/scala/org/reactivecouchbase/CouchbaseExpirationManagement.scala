package org.reactivecouchbase

import scala.concurrent.duration._

/**
 * A bunch of implicit conversion for handling the TTL issue of Couchbase operations.
 * If TTL bigger than 30 days, then provide a UNIX epoch
 **/
object CouchbaseExpiration {
  sealed trait CouchbaseExpirationTiming
  case class CouchbaseExpirationTiming_byInt(value: Int) extends CouchbaseExpirationTiming
  case class CouchbaseExpirationTiming_byDuration(value: Duration) extends CouchbaseExpirationTiming

  implicit def from_CouchabaseGetObject_fromString(a: Int): CouchbaseExpirationTiming = new CouchbaseExpirationTiming_byInt(a)
  implicit def from_CouchabaseGetObject_fromString(a: Duration): CouchbaseExpirationTiming = new CouchbaseExpirationTiming_byDuration(a)

  implicit def from_CouchbaseExpirationTiming_to_int(a: CouchbaseExpirationTiming): Int = a match {
    case i: CouchbaseExpirationTiming_byInt => i.value
    case d: CouchbaseExpirationTiming_byDuration => if (d.value <= 30.days) d.value.toSeconds.toInt else ((System.currentTimeMillis() + d.value.toMillis) / 1000).toInt
  }
}