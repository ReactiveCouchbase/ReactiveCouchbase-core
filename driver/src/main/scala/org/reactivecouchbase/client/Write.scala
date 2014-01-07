package org.reactivecouchbase.client

import org.reactivecouchbase.CouchbaseBucket
import play.api.libs.json.{Json, Writes}
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import net.spy.memcached.{PersistTo, ReplicateTo}
import net.spy.memcached.transcoders.Transcoder
import org.reactivecouchbase.client.CouchbaseFutures._
import play.api.libs.iteratee.{Iteratee, Enumerator}
import org.reactivecouchbase.CouchbaseExpiration._

/**
 * Trait for write operations
 */
trait Write {

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Set Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   *
   *
   *
   * @param value
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def setWithId[T <: {def id:String}](value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](value.id, value, exp, persistTo, replicateTo)(bucket, w, ec)
  }

  /**
   *
   *
   *
   * @param key
   * @param value
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def setWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set[T](key(value), value, exp, persistTo, replicateTo)(bucket, w, ec)
  }

  /**
   *
   *
   *
   * @param key
   * @param value
   * @param tc
   * @param bucket
   * @param ec
   * @tparam T
   * @return
   */
  def set[T](key: String, value: T, tc: Transcoder[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, Constants.expiration, value, tc), bucket, ec )
  }

  /**
   *
   *
   *
   * @param key
   * @param exp
   * @param value
   * @param tc
   * @param bucket
   * @param ec
   * @tparam T
   * @return
   */
  def set[T](key: String, exp: CouchbaseExpirationTiming, value: T, tc: Transcoder[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, exp, value, tc), bucket, ec )
  }

  /**
   *
   *
   *
   * @param key
   * @param value
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def set[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, exp, Json.stringify(w.writes(value)), persistTo, replicateTo), bucket, ec )
  }

  /**
   *
   *
   *
   * @param data
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def setStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    data(Iteratee.fold(List[Future[OperationStatus]]()) { (list, chunk) =>
      list :+ set[T](chunk._1, chunk._2, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  /**
   *
   *
   *
   * @param key
   * @param data
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def setStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    data(Iteratee.fold(List[Future[OperationStatus]]()) { (list, chunk) =>
      list :+ set[T](key(chunk), chunk, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Add Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   *
   *
   *
   * @param value
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def addWithId[T <: {def id:String}](value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](value.id, value, exp, persistTo, replicateTo)(bucket, w, ec)
  }

  /**
   *
   *
   *
   * @param key
   * @param value
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def addWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add[T](key(value), value, exp, persistTo, replicateTo)(bucket, w, ec)
  }

  /**
   *
   *
   *
   * @param key
   * @param value
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def add[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.add(key, exp, Json.stringify(w.writes(value)), persistTo, replicateTo), bucket, ec )
  }

  /**
   *
   *
   *
   * @param data
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def addStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    data(Iteratee.fold(List[Future[OperationStatus]]()) { (list, chunk) =>
      list :+ add[T](chunk._1, chunk._2, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  /**
   *
   *
   *
   * @param key
   * @param data
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def addStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    data(Iteratee.fold(List[Future[OperationStatus]]()) { (list, chunk) =>
      list :+ add[T](key(chunk), chunk, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Replace Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   *
   *
   *
   * @param value
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def replaceWithId[T <: {def id:String}](value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](value.id, value, exp, persistTo, replicateTo)(bucket, w, ec)
  }

  /**
   *
   *
   *
   * @param key
   * @param value
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def replaceWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace[T](key(value), value, exp, persistTo, replicateTo)(bucket, w, ec)
  }

  /**
   *
   *
   *
   * @param key
   * @param value
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def replace[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.replace(key, exp, Json.stringify(w.writes(value)), persistTo, replicateTo), bucket, ec )
  }

  /**
   *
   *
   *
   * @param data
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def replaceStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    data(Iteratee.fold(List[Future[OperationStatus]]()) { (list, chunk) =>
      list :+ replace[T](chunk._1, chunk._2, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  /**
   *
   *
   *
   * @param key
   * @param data
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param bucket
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def replaceStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    data(Iteratee.fold(List[Future[OperationStatus]]()) { (list, chunk) =>
      list :+ replace[T](key(chunk), chunk, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  /**
   *
   * Flush the current bucket
   *
   * @param delay delay to flush
   * @param bucket the current bucket
   * @param ec ExecutionContext for async processing
   * @return the operations tatus
   */
  def flush(delay: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.flush(delay), bucket, ec )
  }

  /**
   *
   * Flush the current bucket
   *
   * @param bucket the current bucket
   * @param ec ExecutionContext for async processing
   * @return the operations tatus
   */
  def flush()(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    flush(Constants.expiration)(bucket, ec)
  }
}
