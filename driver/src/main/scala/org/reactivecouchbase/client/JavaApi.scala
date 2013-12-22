package org.reactivecouchbase.client

import net.spy.memcached.{ReplicateTo, PersistTo}
import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import org.reactivecouchbase.client.CouchbaseFutures._
import com.couchbase.client.protocol.views._
import scala.collection.JavaConversions._

trait JavaApi { self: Queries =>

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Java Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def javaReplace(key: String, exp: Int, value: String, persistTo: PersistTo, replicateTo: ReplicateTo, bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.replace(key, exp, value, persistTo, replicateTo), bucket, ec )
  }

  def javaAdd(key: String, exp: Int, value: String, persistTo: PersistTo, replicateTo: ReplicateTo, bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.add(key, exp, value, persistTo, replicateTo), bucket, ec )
  }

  def javaSet(key: String, exp: Int, value: String, persistTo: PersistTo, replicateTo: ReplicateTo, bucket: CouchbaseBucket, ec: ExecutionContext): Future[OperationStatus] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, exp, value, persistTo, replicateTo), bucket, ec )
  }

  def javaGet(key: String, bucket: CouchbaseBucket, ec: ExecutionContext): Future[String] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key), bucket, ec ).flatMap {
      case s: String => Future.successful(s)
      case _ => Future.failed(new NullPointerException)
    }(ec)
  }

  def javaGet[T](key: String, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[T] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key), bucket, ec ).flatMap {
      case value: String => Future.successful(play.libs.Json.fromJson(play.libs.Json.parse(value), clazz))
      case _ => Future.failed(new NullPointerException)
    }(ec)
  }

  def javaOptGet[T](key: String, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[java.util.List[T]] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key), bucket, ec ).map { f =>
      val opt: java.util.List[T] = f match {
        case value: String => java.util.Collections.singletonList(play.libs.Json.fromJson(play.libs.Json.parse(value), clazz))
        case _ => java.util.Collections.emptyList()
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
    waitForHttp( bucket.couchbaseClient.asyncQuery(view, query), bucket, ec ).map { results =>
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
    waitForHttp( bucket.couchbaseClient.asyncQuery(view, query), bucket, ec ).map { results =>
      asJavaCollection(results.iterator().map {
        case r: ViewRowWithDocs if query.willIncludeDocs() => new Row[T](Some(play.libs.Json.fromJson(play.libs.Json.parse(r.getDocument.asInstanceOf[String]), clazz)), r.getId, r.getKey, r.getValue )
        //case r: ViewRowWithDocs if !query.willIncludeDocs() => new Row[T](id = r.getId, key = r.getKey, value = r.getValue )
        case r: ViewRowNoDocs => new Row[T](d = None, id = r.getId, key = r.getKey, value = r.getValue )
        case r: ViewRowReduced if query.willIncludeDocs() => new Row[T](Some(play.libs.Json.fromJson(play.libs.Json.parse(r.getDocument), clazz)), "", r.getKey, r.getValue )
        case r: ViewRowReduced if !query.willIncludeDocs() => new Row[T](d = None, id = "", key = r.getKey, value = r.getValue )
        case r: SpatialViewRowNoDocs => new Row[T](d = None, id = r.getId, key = r.getKey, value = r.getValue )
        case r: SpatialViewRowWithDocs if query.willIncludeDocs() => new Row[T](Some(play.libs.Json.fromJson(play.libs.Json.parse(r.getDocument.asInstanceOf[String]), clazz)), r.getId, r.getKey, r.getValue )
        case r: SpatialViewRowWithDocs if !query.willIncludeDocs() => new Row[T](d = None, id = r.getId, key = r.getKey, value = r.getValue )
        //new Row[T](play.libs.Json.fromJson(play.libs.Json.parse(r.getDocument.asInstanceOf[String]), clazz), r.getId, r.getKey, r.getValue )
      }.toList)
    }(ec)
  }

  def javaView(docName: String, viewName: String, bucket: CouchbaseBucket, ec: ExecutionContext): Future[View] = {
    waitForHttp[View]( bucket.couchbaseClient.asyncGetView(docName, viewName), bucket, ec )
  }

  def asJavaLong(f: Future[Long], ec: ExecutionContext): Future[java.lang.Long] = {
    f.map { (v: Long) =>
      val value: java.lang.Long = v.asInstanceOf[java.lang.Long]
      value
    }(ec)
  }

  def asJavaInt(f: Future[Int], ec: ExecutionContext): Future[java.lang.Integer] = {
    f.map { (v: Int) =>
      val value: java.lang.Integer = v.asInstanceOf[java.lang.Integer]
      value
    }(ec)
  }
}

class Row[T](d: Option[T], val id: String, val key: String, val value: String) {

  def document = d.getOrElse(null)

  private[reactivecouchbase] def docOpt = d

  override def toString: String = {
    "Row{" + "document=" + d.get + ", id='" + id + '\'' + ", key='" + key + '\'' + ", value='" + value + '\'' + '}'
  }

  def isDocumentSet: Boolean = {
    d.isDefined
  }

  override def equals(o: Any): Boolean = {
    if (this.equals(o)) return true
    if (!(o.isInstanceOf[Row[_]])) return false
    val row: Row[_] = o.asInstanceOf[Row[_]]
    if (if (d.isDefined) !(d.get == row.docOpt.get) else row.docOpt.isDefined) return false
    if (if (id != null) !(id == row.id) else row.id != null) return false
    if (if (key != null) !(key == row.key) else row.key != null) return false
    if (if (value != null) !(value == row.value) else row.value != null) return false
    return true
  }

  override def hashCode: Int = {
    var result: Int = if (d.isDefined) d.get.hashCode else 0
    result = 31 * result + (if (id != null) id.hashCode else 0)
    result = 31 * result + (if (key != null) key.hashCode else 0)
    result = 31 * result + (if (value != null) value.hashCode else 0)
    return result
  }
}
