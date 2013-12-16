package org.reactivecouchbase


import scala.concurrent.duration._


/*
 * Handling the expiration problem
 * 
 */
object CouchbaseExpiration {
  sealed trait CouchbaseExpirationTiming
  case class CouchbaseExpirationTiming_byInt(value: Int) extends CouchbaseExpirationTiming
  case class CouchbaseExpirationTiming_byDuration(value: Duration) extends CouchbaseExpirationTiming

  implicit def from_CouchabaseGetObject_fromString(a: Int): CouchbaseExpirationTiming = new CouchbaseExpirationTiming_byInt(a)
  implicit def from_CouchabaseGetObject_fromString(a: Duration): CouchbaseExpirationTiming = new CouchbaseExpirationTiming_byDuration(a)

  implicit def from_CouchbaseExpirationTiming_to_int(a: CouchbaseExpirationTiming): Int = a match {
    case i: CouchbaseExpirationTiming_byInt => i.value
    case d: CouchbaseExpirationTiming_byDuration => if (d.value < 30.days) d.value.toSeconds.toInt else { (System.currentTimeMillis() + d.value.toMillis).toInt / 1000 }
  }
}