package org.reactivecouchbase.client

import net.spy.memcached.{ReplicateTo, PersistTo}
import org.reactivecouchbase.{Configuration, Logger}

object Constants {
  val expiration: Int = -1
  val jsonStrictValidation = Configuration.getBoolean("couchbase.json.validate").getOrElse(true)
  val failWithOpStatus = Configuration.getBoolean("couchbase.failfutures").getOrElse(false)
  val timeout: Long = Configuration.getLong("couchbase.execution-context.timeout").getOrElse(1000L)
  implicit val defaultPersistTo: PersistTo = PersistTo.ZERO
  implicit val defaultReplicateTo: ReplicateTo = ReplicateTo.ZERO
  if (jsonStrictValidation) {
    Logger.info("Failing on bad JSON structure enabled.")
  }
  if (failWithOpStatus) {
    Logger.info("Failing Futures on failed OperationStatus enabled.")
  }
}