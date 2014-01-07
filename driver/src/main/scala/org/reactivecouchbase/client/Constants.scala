package org.reactivecouchbase.client

import net.spy.memcached.{ReplicateTo, PersistTo}

/**
 * Some constants
 */
object Constants {

  /**
   * Infinity persistence
   */
  val expiration: Int = -1

  /**
   * Standard PersistTo
   */
  implicit val defaultPersistTo: PersistTo = PersistTo.ZERO

  /**
   * Standard ReplicateTo
   */
  implicit val defaultReplicateTo: ReplicateTo = ReplicateTo.ZERO
}