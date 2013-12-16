package org.reactivecouchbase.client

import net.spy.memcached.{ReplicateTo, PersistTo}
import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import org.reactivecouchbase.client.CouchbaseFutures._
import com.couchbase.client.protocol.views._
import scala.collection.JavaConversions._
import org.ancelin.play2.java.couchbase.Row

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
    waitForGet( bucket.couchbaseClient.asyncGet(key), ec ).flatMap { f =>
      f match {
        case value: String => Future.successful(play.libs.Json.fromJson(play.libs.Json.parse(value), clazz))
        case _ => Future.failed(new NullPointerException)
      }
    }(ec)
  }

  def javaOptGet[T](key: String, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[play.libs.F.Option[T]] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key), ec ).map { f =>
      val opt: play.libs.F.Option[T] = f match {
        case value: String => play.libs.F.Option.Some[T](play.libs.Json.fromJson(play.libs.Json.parse(value), clazz))
        case _ => play.libs.F.Option.None[T]()
      }
      opt
    }(ec)
  }

  def javaFind[T](docName:String, viewName: String, query: Query, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[java.util.Collection[T]] = {
    view(docName, viewName)(bucket, ec).flatMap { view =>
      javaFind[T](view, query, clazz, bucket, ec)
    }(ec)
  }

  def javaFind[T](view: View, query: Query, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[java.util.Collection[T]] = {
    waitForHttp( bucket.couchbaseClient.asyncQuery(view, query), ec ).map { results =>
      asJavaCollection(results.iterator().collect {
        case r: ViewRowWithDocs if query.willIncludeDocs() => play.libs.Json.fromJson(play.libs.Json.parse(r.getDocument.asInstanceOf[String]), clazz)
        case r: ViewRowReduced if query.willIncludeDocs() => play.libs.Json.fromJson(play.libs.Json.parse(r.getDocument), clazz)
        case r: SpatialViewRowWithDocs if query.willIncludeDocs() => play.libs.Json.fromJson(play.libs.Json.parse(r.getDocument.asInstanceOf[String]), clazz)
        //play.libs.Json.fromJson(play.libs.Json.parse(r.getDocument.asInstanceOf[String]), clazz)
      }.toList)
    }(ec)
  }

  def javaFullFind[T](docName:String, viewName: String, query: Query, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[java.util.Collection[Row[T]]] = {
    view(docName, viewName)(bucket, ec).flatMap { view =>
      javaFullFind[T](view, query, clazz, bucket, ec)
    }(ec)
  }

  def javaFullFind[T](view: View, query: Query, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[java.util.Collection[Row[T]]] = {
    waitForHttp( bucket.couchbaseClient.asyncQuery(view, query), ec ).map { results =>
      asJavaCollection(results.iterator().map {
        case r: ViewRowWithDocs if query.willIncludeDocs() => new Row[T](play.libs.Json.fromJson(play.libs.Json.parse(r.getDocument.asInstanceOf[String]), clazz), r.getId, r.getKey, r.getValue )
        case r: ViewRowWithDocs if !query.willIncludeDocs() => new Row[T](r.getId, r.getKey, r.getValue )
        case r: ViewRowNoDocs => new Row[T](r.getId, r.getKey, r.getValue )
        case r: ViewRowReduced if query.willIncludeDocs() => new Row[T](play.libs.Json.fromJson(play.libs.Json.parse(r.getDocument), clazz), "", r.getKey, r.getValue )
        case r: ViewRowReduced if !query.willIncludeDocs() => new Row[T]("", r.getKey, r.getValue )
        case r: SpatialViewRowNoDocs => new Row[T](r.getId, r.getKey, r.getValue )
        case r: SpatialViewRowWithDocs if query.willIncludeDocs() => new Row[T](play.libs.Json.fromJson(play.libs.Json.parse(r.getDocument.asInstanceOf[String]), clazz), r.getId, r.getKey, r.getValue )
        case r: SpatialViewRowWithDocs if !query.willIncludeDocs() => new Row[T](r.getId, r.getKey, r.getValue )
        //new Row[T](play.libs.Json.fromJson(play.libs.Json.parse(r.getDocument.asInstanceOf[String]), clazz), r.getId, r.getKey, r.getValue )
      }.toList)
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
