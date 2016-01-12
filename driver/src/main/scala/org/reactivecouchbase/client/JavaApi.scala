package org.reactivecouchbase.client

import net.spy.memcached.transcoders.Transcoder
import net.spy.memcached.{ReplicateTo, PersistTo}
import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Future, ExecutionContext}
import org.reactivecouchbase.client.CouchbaseFutures._
import com.couchbase.client.protocol.views._
import scala.collection.JavaConversions._
import play.api.libs.json.Json

/**
 * Trait for Java API (for Play framework usage)
 */
trait JavaApi { self: Queries =>

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Java Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /**
   *
   * Replace a document
   *
   * @param key the key of the doc
   * @param exp expiration of the doc
   * @param value the document
   * @param persistTo persistance flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the operation status
   */
  def javaReplace(key: String, exp: Int, value: String, persistTo: PersistTo, replicateTo: ReplicateTo, bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.replace(key, exp, value, persistTo, replicateTo), bucket, ec ).map(OpResult(_, 1, Some(Json.parse(value))))(ec)
  }

  /**
   *
   * Add a document
   *
   * @param key the key of the doc
   * @param exp expiration of the doc
   * @param value the document
   * @param persistTo persistance flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the operation status
   */
  def javaAdd(key: String, exp: Int, value: String, persistTo: PersistTo, replicateTo: ReplicateTo, bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.add(key, exp, value, persistTo, replicateTo), bucket, ec ).map(OpResult(_, 1, Some(Json.parse(value))))(ec)
  }

  /**
   *
   * Set a document
   *
   * @param key the key of the doc
   * @param exp expiration of the doc
   * @param value the document
   * @param tc the transcoder
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the operation status
   */
  def javaSet[T](key: String, exp: Int, value: T, tc: Transcoder[T], bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, exp, value, tc), bucket, ec ).map(OpResult(_, 1))(ec)
  }

  /**
   *
   * Set a document
   *
   * @param key the key of the doc
   * @param exp expiration of the doc
   * @param value the document
   * @param persistTo persistance flag
   * @param replicateTo replication flag
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the operation status
   */
  def javaSet(key: String, exp: Int, value: String, persistTo: PersistTo, replicateTo: ReplicateTo, bucket: CouchbaseBucket, ec: ExecutionContext):  Future[OpResult] = {
    waitForOperationStatus( bucket.couchbaseClient.set(key, exp, value, persistTo, replicateTo), bucket, ec ).map(OpResult(_, 1, Some(Json.parse(value))))(ec)
  }

  /**
   *
   * Fetch a raw document
   *
   * @param key the key of the doc
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the raw document
   */
  def javaGet(key: String, bucket: CouchbaseBucket, ec: ExecutionContext): Future[String] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key), bucket, ec ).flatMap {
      case s: String => Future.successful(s)
      case null => Future.failed(new IllegalStateException(s"Nothing found for key : $key"))
      case _ => Future.failed(new IllegalStateException(s"Document $key is not a string ..."))
    }(ec)
  }

  /**
   *
   * Fetch a document
   *
   * @param key the key of the doc
   * @param clazz the class of the doc
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the document
   */
  def javaGet[T](key: String, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[T] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key), bucket, ec ).flatMap {
      case value: String => Future.successful(play.libs.Json.fromJson(play.libs.Json.parse(value), clazz))
      case null => Future.failed(new IllegalStateException(s"Nothing found for key : $key"))
      case _ => Future.failed(new IllegalStateException(s"Document $key is not a string"))
    }(ec)
  }

  /**
   * Fetch a document
   * Applies the transcoder of the row document.
   * @param key the key of the doc
   * @param tc the transcoder
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the document
   * @return
   */
  def javaGet[T](key: String, tc: Transcoder[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[T] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key, tc), bucket, ec ).flatMap {
      case value: T => Future.successful(value)
      case _ => Future.failed(new IllegalStateException(s"Document $key does not exist or could not parsed by ${tc.getClass}"))
    }(ec)
  }

  /**
   *
   * Fetch an optional document
   *
   * @param key the key of the doc
   * @param clazz the class of the doc
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the list of docs
   */
  def javaOptGet[T](key: String, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[java.util.List[T]] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key), bucket, ec ).map { f =>
      val opt: java.util.List[T] = f match {
        case value: String => java.util.Collections.singletonList(play.libs.Json.fromJson(play.libs.Json.parse(value), clazz))
        case null => java.util.Collections.emptyList()
        case _ => if (bucket.failWithNonStringDoc) throw new IllegalStateException(s"Document $key is not a string") else java.util.Collections.emptyList()
      }
      opt
    }(ec)
  }

  /**
   *
   * Perform a query
   *
   * @param docName design document name
   * @param viewName the name of the view
   * @param query the actual query
   * @param clazz the class of the doc
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return the list of docs
   */
  def javaFind[T](docName:String, viewName: String, query: Query, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[java.util.Collection[T]] = {
    view(docName, viewName)(bucket, ec).flatMap { view =>
      javaFind[T](view, query, clazz, bucket, ec)
    }(ec)
  }

  /**
   *
   * Perform a query
   *
   * @param view the view to query
   * @param query the actual query
   * @param clazz the class of the doc
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the list of docs
   */
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

  /**
   *
   * Perform a query
   *
   * @param docName design document name
   * @param viewName the name of the view
   * @param query the actual query
   * @param clazz the class of the doc
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the list of rows
   */
  def javaFullFind[T](docName:String, viewName: String, query: Query, clazz: Class[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[java.util.Collection[Row[T]]] = {
    view(docName, viewName)(bucket, ec).flatMap { view =>
      javaFullFind[T](view, query, clazz, bucket, ec)
    }(ec)
  }

  /**
   *
   * Perform a query
   *
   * @param view the view to query
   * @param query the actual query
   * @param clazz the class of the doc
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T the type of the doc
   * @return the list of rows
   */
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

  /**
   *
   * Fetch view
   *
   * @param docName the design doc name
   * @param viewName the name of the view
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return the view
   */
  def javaView(docName: String, viewName: String, bucket: CouchbaseBucket, ec: ExecutionContext): Future[View] = {
    waitForHttp[View]( bucket.couchbaseClient.asyncGetView(docName, viewName), bucket, ec )
  }

  /**
   *
   * Java long conversion
   *
   * @param f future to cast
   * @param ec ExecutionContext for async processing
   * @return casted future
   */
  def asJavaLong(f: Future[Long], ec: ExecutionContext): Future[java.lang.Long] = {
    f.map { (v: Long) =>
      val value: java.lang.Long = v.asInstanceOf[java.lang.Long]
      value
    }(ec)
  }

  /**
   *
   * Java int conversion
   *
   * @param f future to cast
   * @param ec ExecutionContext for async processing
   * @return casted future
   */
  def asJavaInt(f: Future[Int], ec: ExecutionContext): Future[java.lang.Integer] = {
    f.map { (v: Int) =>
      val value: java.lang.Integer = v.asInstanceOf[java.lang.Integer]
      value
    }(ec)
  }
}

/**
 *
 * Result row representation
 *
 * @param d the document
 * @param id the key of the doc
 * @param key the indexed key
 * @param value the indexed value
 * @tparam T type of the doc
 */
class Row[T](d: Option[T], val id: String, val key: String, val value: String) {

  /**
   * @return the document
   */
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
