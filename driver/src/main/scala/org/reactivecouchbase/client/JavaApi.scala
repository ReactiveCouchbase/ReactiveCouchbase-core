package org.reactivecouchbase.client

import net.spy.memcached.{ReplicateTo, PersistTo}
import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import org.reactivecouchbase.client.CouchbaseFutures._
import com.couchbase.client.protocol.views._

trait JavaApi { self: Queries =>

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Java Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def javaReplace(key: String, exp: Int, value: String, persistTo: PersistTo, replicateTo: ReplicateTo, bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.replace(key, exp, value, persistTo, replicateTo), ec )
  }

  def javaAdd(key: String, exp: Int, value: String, persistTo: PersistTo, replicateTo: ReplicateTo, bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.add(key, exp, value, persistTo, replicateTo), ec )
  }

  def javaSet(key: String, exp: Int, value: String, persistTo: PersistTo, replicateTo: ReplicateTo, bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, exp, value, persistTo, replicateTo), ec )
  }

  def javaGet(key: String, bucket: CouchbaseBucket, ec: ExecutionContext): Future[String] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key), ec ).flatMap {
      case s: String => Future.successful(s)
      case _ => Future.failed(new NullPointerException)
    }(ec)
  }

  def javaGet[T](key: String, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[T] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key), ec ).flatMap {
      case value: String => Future.successful(play.libs.Json.fromJson(play.libs.Json.parse(value), clazz))
      case _ => Future.failed(new NullPointerException)
    }(ec)
  }

  def javaView(docName: String, viewName: String, bucket: CouchbaseBucket, ec: ExecutionContext): Future[View] = {
    waitForHttp[View]( bucket.couchbaseClient.asyncGetView(docName, viewName), ec )
  }

  def asJavaLong(f: Future[Long], ec: ExecutionContext): Future[java.lang.Long] = {
    f.map { (v: Long) => v: java.lang.Long }(ec)
  }

  def asJavaInt(f: Future[Int], ec: ExecutionContext): Future[java.lang.Integer] = {
    f.map { (v: Int) => v: java.lang.Integer }(ec)
  }
}
