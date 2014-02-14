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
   * Set a document that contains an id field
   *
   * @param value the document
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def setWithId[T <: {def id:String}](value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext):  Future[OpResult] = {
    set[T](value.id, value, exp, persistTo, replicateTo)(bucket, w, ec)
  }

  /**
   *
   * Set a document
   *
   * @param key the key of the document
   * @param value the document
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def setWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext):  Future[OpResult] = {
    set[T](key(value), value, exp, persistTo, replicateTo)(bucket, w, ec)
  }

  /**
   *
   * Set a document
   *
   * @param key the key of the document
   * @param value the document
   * @param tc transcoder
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def set[T](key: String, value: T, tc: Transcoder[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, Constants.expiration, value, tc), bucket, ec ).map(OpResult(_, 1))
  }

  /**
   *
   * Set a document
   *
   * @param key the key of the document
   * @param exp expiration of the doc
   * @param value the document
   * @param tc transcoder
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def set[T](key: String, exp: CouchbaseExpirationTiming, value: T, tc: Transcoder[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, exp, value, tc), bucket, ec ).map(OpResult(_, 1))
  }

  /**
   *
   * Set a document
   *
   * @param key the key of the document
   * @param value the document
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def set[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, exp, Json.stringify(w.writes(value)), persistTo, replicateTo), bucket, ec ).map(OpResult(_, 1, Some(w.writes(value))))
  }

  /**
   *
   * Set a stream of documents
   *
   * @param data the stream of documents
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def setStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[OpResult]] = {
    data(Iteratee.fold(List[ Future[OpResult]]()) { (list, chunk) =>
      list :+ set[T](chunk._1, chunk._2, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  /**
   *
   * Set a stream of documents
   *
   * @param key the key of the document
   * @param data the stream of documents
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def setStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[OpResult]] = {
    data(Iteratee.fold(List[ Future[OpResult]]()) { (list, chunk) =>
      list :+ set[T](key(chunk), chunk, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Add Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   *
   * Add a document
   *
   * @param value the document
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def addWithId[T <: {def id:String}](value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext):  Future[OpResult] = {
    add[T](value.id, value, exp, persistTo, replicateTo)(bucket, w, ec)
  }

  /**
   *
   * Add a document
   *
   * @param key the key of the document
   * @param value the document
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def addWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext):  Future[OpResult] = {
    add[T](key(value), value, exp, persistTo, replicateTo)(bucket, w, ec)
  }

  /**
   *
   * Add a document
   *
   * @param key the key of the document
   * @param value the document
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def add[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.add(key, exp, Json.stringify(w.writes(value)), persistTo, replicateTo), bucket, ec ).map(OpResult(_, 1, Some(w.writes(value))))
  }

  /**
   *
   * Add a stream of documents
   *
   * @param data the stream of documents
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def addStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[OpResult]] = {
    data(Iteratee.fold(List[ Future[OpResult]]()) { (list, chunk) =>
      list :+ add[T](chunk._1, chunk._2, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  /**
   *
   * Add a stream of documents
   *
   * @param key the key of the document
   * @param data the stream of documents
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def addStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[OpResult]] = {
    data(Iteratee.fold(List[ Future[OpResult]]()) { (list, chunk) =>
      list :+ add[T](key(chunk), chunk, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Replace Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   *
   * Replace a document
   *
   * @param value the document
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def replaceWithId[T <: {def id:String}](value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext):  Future[OpResult] = {
    replace[T](value.id, value, exp, persistTo, replicateTo)(bucket, w, ec)
  }

  /**
   *
   * Replace a document
   *
   * @param key the key of the document
   * @param value the document
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def replaceWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext):  Future[OpResult] = {
    replace[T](key(value), value, exp, persistTo, replicateTo)(bucket, w, ec)
  }

  /**
   *
   * Replace a document
   *
   * @param key the key of the document
   * @param value the document
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def replace[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.replace(key, exp, Json.stringify(w.writes(value)), persistTo, replicateTo), bucket, ec ).map(OpResult(_, 1, Some(w.writes(value))))
  }

  /**
   *
   * Replace a stream documents
   *
   * @param data the stream of documents
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def replaceStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[OpResult]] = {
    data(Iteratee.fold(List[ Future[OpResult]]()) { (list, chunk) =>
      list :+ replace[T](chunk._1, chunk._2, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  /**
   *
   * Replace a stream documents
   *
   * @param key the key of the document
   * @param data the stream of documents
   * @param exp expiration of the doc
   * @param persistTo persistence flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the operation status
   */
  def replaceStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[OpResult]] = {
    data(Iteratee.fold(List[ Future[OpResult]]()) { (list, chunk) =>
      list :+ replace[T](key(chunk), chunk, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  /**
   *
   * Flush the current bucket
   *
   * @param delay delay to flush
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the operation status
   */
  def flush(delay: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.flush(delay), bucket, ec ).map(OpResult(_, 1))
  }

  /**
   *
   * Flush the current bucket
   *
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the operation status
   */
  def flush()(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    flush(Constants.expiration)(bucket, ec)
  }
}
