package org.reactivecouchbase.client

import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Future, ExecutionContext}
import org.reactivecouchbase.client.CouchbaseFutures._

/**
 * Trait for number operations
 */
trait Counters {

  def getInt(key: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Int] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key), bucket, ec ) flatMap {
      case i: java.lang.Integer => Future.successful(i.toInt)
      case _ => Future.failed(new IllegalStateException("Value isn't an int"))
    }
  }

  def getLong(key: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Long] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key), bucket, ec ) flatMap {
      case i: java.lang.Long => Future.successful(i.toLong)
      case _ => Future.failed(new IllegalStateException("Value isn't a long"))
    }
  }

  def setInt(key: String, value: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, value: java.lang.Integer), bucket, ec).map(OpResult(_, 1))
  }

  def setLong(key: String, value: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, value: java.lang.Long), bucket, ec).map(OpResult(_, 1))
  }

  /**
   *
   * Increment an Int
   *
   * @param key key of the Int value
   * @param by increment of the value
   * @param bucket bucket to use
   * @param ec ExecutionContext for async processing
   * @return
   */
  def incr(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.asyncIncr(key, by: java.lang.Integer), bucket, ec ).map(OpResult(_, 1))
  }

  /**
   *
   * Increment a Long
   *
   * @param key key of the Long value
   * @param by the value to increment
   * @param bucket bucket to use
   * @param ec ExecutionContext for async processing
   * @return
   */
  def incr(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.asyncIncr(key, by: java.lang.Long), bucket, ec ).map(OpResult(_, 1))
  }

  /**
   *
   * Decrement an Int
   *
   * @param key key of the Int value
   * @param by the value to decrement
   * @param bucket bucket to use
   * @param ec ExecutionContext for async processing
   * @return
   */
  def decr(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.asyncDecr(key, by: java.lang.Integer), bucket, ec ).map(OpResult(_, 1))
  }

  /**
   *
   * Decrement a Long
   *
   * @param key key of the Long value
   * @param by the value to decrement
   * @param bucket bucket to use
   * @param ec ExecutionContext for async processing
   * @return
   */
  def decr(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.asyncDecr(key, by: java.lang.Long), bucket, ec ).map(OpResult(_, 1))
  }

  /**
   *
   * Increment and get an Int
   *
   * @param key key of the Int value
   * @param by the value to increment
   * @param bucket bucket to use
   * @param ec ExecutionContext for async processing
   * @return
   */
  def incrAndGet(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Int] = {
    incr(key, by)(bucket, ec).flatMap(_ => getInt(key)(bucket, ec))
  }

  /**
   *
   * Increment and get a Long
   *
   * @param key key of the Long value
   * @param by the value to increment
   * @param bucket bucket to use
   * @param ec ExecutionContext for async processing
   * @return
   */
  def incrAndGet(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Long] = {
    incr(key, by)(bucket, ec).flatMap(_ => getLong(key)(bucket, ec))
  }

  /**
   *
   * Decrement and get an Int
   *
   * @param key key of the Int value
   * @param by the value to decrement
   * @param bucket bucket to use
   * @param ec ExecutionContext for async processing
   * @return
   */
  def decrAndGet(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Int] = {
    decr(key, by)(bucket, ec).flatMap(_ => getInt(key)(bucket, ec))
  }

  /**
   *
   * Decrement and get a Long
   *
   * @param key key of the Long value
   * @param by the value to decrement
   * @param bucket bucket to use
   * @param ec ExecutionContext for async processing
   * @return
   */
  def decrAndGet(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Long] = {
    decr(key, by)(bucket, ec).flatMap(_ => getLong(key)(bucket, ec))
  }
}
