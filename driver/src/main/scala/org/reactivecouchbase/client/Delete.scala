package org.reactivecouchbase.client

import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import org.reactivecouchbase.client.CouchbaseFutures._
import net.spy.memcached.{PersistTo, ReplicateTo}
import play.api.libs.iteratee.{Iteratee, Enumerator}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Delete Operations
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
trait Delete {

  def delete(key: String, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(key, persistTo, replicateTo), ec )
  }

  def deleteWithId[T <: {def id:String}](value: T, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(value.id, persistTo, replicateTo), ec )
  }

  def deleteWithKey[T](key: T => String, value: T, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.delete(key(value), persistTo, replicateTo), ec )
  }

  def deleteStream(data: Enumerator[String], persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[List[OperationStatus]] = {
    data(Iteratee.fold(List[Future[OperationStatus]]()) { (list, chunk) =>
      list :+ delete(chunk, persistTo, replicateTo)(bucket, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  def deleteStreamWithKey[T](key: T => String, data: Enumerator[T], persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[List[OperationStatus]] = {
    data(Iteratee.fold(List[Future[OperationStatus]]()) { (list, chunk) =>
      list :+ delete(key(chunk), persistTo, replicateTo)(bucket, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }
}
