package org.reactivecouchbase.client

import play.api.Play
import play.api.Play.current
import net.spy.memcached.{ReplicateTo, PersistTo}

object Constants {
  val expiration: Int = -1
  val jsonStrictValidation = Play.configuration.getBoolean("couchbase.json.validate").getOrElse(true)
  val usePlayEC = Play.configuration.getBoolean("couchbase.useplayec").getOrElse(true)
  val failWithOpStatus = Play.configuration.getBoolean("couchbase.failfutures").getOrElse(false)
  val timeout: Long = Play.configuration.getLong("couchbase.execution-context.timeout").getOrElse(1000L)
  implicit val defaultPersistTo: PersistTo = PersistTo.ZERO
  implicit val defaultReplicateTo: ReplicateTo = ReplicateTo.ZERO
  if (jsonStrictValidation) {
    play.api.Logger("CouchbasePlugin").info("Failing on bad JSON structure enabled.")
  }
  if (failWithOpStatus) {
    play.api.Logger("CouchbasePlugin").info("Failing Futures on failed OperationStatus enabled.")
  }
}