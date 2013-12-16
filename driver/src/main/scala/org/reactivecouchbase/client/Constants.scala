package org.reactivecouchbase.client

import net.spy.memcached.{ReplicateTo, PersistTo}
import org.reactivecouchbase.{Configuration, Logger}

object Constants {
  val expiration: Int = -1
  implicit val defaultPersistTo: PersistTo = PersistTo.ZERO
  implicit val defaultReplicateTo: ReplicateTo = ReplicateTo.ZERO
}