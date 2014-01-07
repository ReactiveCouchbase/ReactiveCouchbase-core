package org.reactivecouchbase.client

import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import org.reactivecouchbase.client.CouchbaseFutures._

/**
 * Trait for number operations
 */
trait Counters {

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
  def incr(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.asyncIncr(key, by), bucket, ec )
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
  def incr(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.asyncIncr(key, by), bucket, ec )
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
  def decr(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.asyncDecr(key, by), bucket, ec )
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
  def decr(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.asyncDecr(key, by), bucket, ec )
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
    Future[Long]( bucket.couchbaseClient.incr(key, by) )(ec).map(_.toInt)
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
    Future[Long]( bucket.couchbaseClient.incr(key, by) )(ec)
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
    Future[Long]( bucket.couchbaseClient.decr(key, by) )(ec).map(_.toInt)
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
    Future[Long]( bucket.couchbaseClient.decr(key, by) )(ec)
  }
}
