package org.reactivecouchbase.client

import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import org.reactivecouchbase.client.CouchbaseFutures._
import net.spy.memcached.{PersistTo, ReplicateTo}
import play.api.libs.iteratee.{Iteratee, Enumerator}

/**
 * Trait for delete operations
 */
trait Delete {

  /**
   *
   * Delete a document
   *
   * @param key the key to delete
   * @param persistTo persist flag
   * @param replicateTo repplication flag
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the operation status for the delete operation
   */
  def delete(key: String, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(key, persistTo, replicateTo), bucket, ec )
  }

  /**
   *
   * Delete a document with an id field of type String
   *
   * @param value
   * @param persistTo persist flag
   * @param replicateTo repplication flag
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T type of documents
   * @return the operation status for the delete operation
   */
  def deleteWithId[T <: {def id:String}](value: T, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(value.id, persistTo, replicateTo), bucket, ec )
  }

  /**
   *
   * Delete a document
   *
   * @param key the extractor to get the key to delete
   * @param value the document
   * @param persistTo persist flag
   * @param replicateTo repplication flag
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T type of document
   * @return the operation status for the delete operation
   */
  def deleteWithKey[T](key: T => String, value: T, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(key(value), persistTo, replicateTo), bucket, ec )
  }

  /**
   *
   * Delete a stream of documents
   *
   * @param data the stream of documents to delete
   * @param persistTo persist flag
   * @param replicateTo repplication flag
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the operation statuses for the delete operation
   */
  def deleteStream(data: Enumerator[String], persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[List[OperationStatus]] = {
    data(Iteratee.fold(List[Future[OperationStatus]]()) { (list, chunk) =>
      list :+ delete(chunk, persistTo, replicateTo)(bucket, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  /**
   *
   * Delete a stream of documents
   *
   * @param key the extractor to get the key
   * @param data the stream of documents to delete
   * @param persistTo persist flag
   * @param replicateTo repplication flag
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T type of document
   * @return the operation statuses for the delete operation
   */
  def deleteStreamWithKey[T](key: T => String, data: Enumerator[T], persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[List[OperationStatus]] = {
    data(Iteratee.fold(List[Future[OperationStatus]]()) { (list, chunk) =>
      list :+ delete(key(chunk), persistTo, replicateTo)(bucket, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }
}
