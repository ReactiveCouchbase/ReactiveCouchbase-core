package org.reactivecouchbase.client

import net.spy.memcached.{ReplicateTo, PersistTo}

object Constants {
  val expiration: Int = -1
  implicit val defaultPersistTo: PersistTo = PersistTo.ZERO
  implicit val defaultReplicateTo: ReplicateTo = ReplicateTo.ZERO
}