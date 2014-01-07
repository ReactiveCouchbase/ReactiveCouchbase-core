package org.reactivecouchbase.client

import play.api.libs.json._
import scala.concurrent.{ Future, ExecutionContext }
import com.couchbase.client.protocol.views.{ DesignDocument, SpatialView, View, Query }
import play.api.libs.iteratee.Enumerator
import net.spy.memcached.ops.OperationStatus
import play.api.libs.json.JsObject
import net.spy.memcached.{ PersistTo, ReplicateTo }
import org.reactivecouchbase.{ Couchbase, CouchbaseBucket }
import java.util.concurrent.TimeUnit
import net.spy.memcached.CASValue
import org.reactivecouchbase.CouchbaseExpiration._

/**
 * Trait containing the whole ReactiveCouchbase API to put on CouchbaseBucket
 */
trait BucketAPI {
  self: CouchbaseBucket =>

  /**
   *
   *
   *
   * @param docName
   * @param viewName
   * @param query
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def find[T](docName: String, viewName: String)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    Couchbase.find[T](docName, viewName)(query)(self, r, ec)
  }

  /**
   *
   *
   *
   * @param view
   * @param query
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def find[T](view: View)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    Couchbase.find[T](view)(query)(self, r, ec)
  }

  /**
   *
   *
   *
   * @param doc
   * @param view
   * @param query
   * @param everyMillis
   * @param filter
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def pollQuery[T](doc: String, view: String, query: Query, everyMillis: Long, filter: T => Boolean = { chunk: T => true })(implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Couchbase.pollQuery[T](doc, view, query, everyMillis, filter)(self, r, ec)
  }

  /**
   *
   *
   *
   * @param doc
   * @param view
   * @param query
   * @param filter
   * @param trigger
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def repeatQuery[T](doc: String, view: String, query: Query, filter: T => Boolean = { chunk: T => true }, trigger: Future[AnyRef] = Future.successful(Some))(implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Couchbase.repeatQuery[T](doc, view, query, trigger, filter)(self, r, ec)
  }

  /**
   *
   *
   *
   * @param doc
   * @param view
   * @param extractor
   * @param from
   * @param every
   * @param unit
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def tailableQuery[T](doc: String, view: String, extractor: T => Long, from: Long = 0L, every: Long = 1000L, unit: TimeUnit = TimeUnit.MILLISECONDS)(implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Couchbase.tailableQuery[T](doc, view, extractor, from, every, unit)(self, r, ec)
  }

  /**
   *
   *
   *
   * @param keysEnumerator
   * @param ec
   * @return
   */
  def rawFetch(keysEnumerator: Enumerator[String])(implicit ec: ExecutionContext): QueryEnumerator[(String, String)] = Couchbase.rawFetch(keysEnumerator)(this, ec)

  /**
   *
   *
   *
   * @param keysEnumerator
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def fetch[T](keysEnumerator: Enumerator[String])(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = Couchbase.fetch[T](keysEnumerator)(this, r, ec)

  /**
   *
   *
   *
   * @param keysEnumerator
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def fetchValues[T](keysEnumerator: Enumerator[String])(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = Couchbase.fetchValues[T](keysEnumerator)(this, r, ec)

  /**
   *
   *
   *
   * @param keys
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def fetch[T](keys: Seq[String])(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = Couchbase.fetch[T](keys)(this, r, ec)

  /**
   *
   *
   *
   * @param keys
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def fetchValues[T](keys: Seq[String])(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = Couchbase.fetchValues[T](keys)(this, r, ec)

  /**
   *
   *
   *
   * @param key
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def getWithKey[T](key: String)(implicit r: Reads[T], ec: ExecutionContext): Future[Option[(String, T)]] = Couchbase.getWithKey[T](key)(this, r, ec)

  /**
   *
   *
   *
   * @param docName
   * @param viewName
   * @param query
   * @param ec
   * @return
   */
  def rawSearch(docName: String, viewName: String)(query: Query)(implicit ec: ExecutionContext): QueryEnumerator[RawRow] = Couchbase.rawSearch(docName, viewName)(query)(this, ec)

  /**
   *
   *
   *
   * @param view
   * @param query
   * @param ec
   * @return
   */
  def rawSearch(view: View)(query: Query)(implicit ec: ExecutionContext): QueryEnumerator[RawRow] = Couchbase.rawSearch(view)(query)(this, ec)

  /**
   *
   *
   *
   * @param docName
   * @param viewName
   * @param query
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def search[T](docName: String, viewName: String)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = Couchbase.search[T](docName, viewName)(query)(this, r, ec)

  /**
   *
   *
   *
   * @param view
   * @param query
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def search[T](view: View)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = Couchbase.search[T](view)(query)(this, r, ec)

  /**
   *
   *
   *
   * @param docName
   * @param viewName
   * @param query
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def searchValues[T](docName: String, viewName: String)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = Couchbase.searchValues[T](docName, viewName)(query)(this, r, ec)

  /**
   *
   *
   *
   * @param view
   * @param query
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def searchValues[T](view: View)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = Couchbase.searchValues[T](view)(query)(this, r, ec)

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /**
   *
   *
   *
   * @param docName
   * @param viewName
   * @param ec
   * @return
   */
  def view(docName: String, viewName: String)(implicit ec: ExecutionContext): Future[View] = {
    Couchbase.view(docName, viewName)(self, ec)
  }

  /**
   *
   *
   *
   * @param docName
   * @param viewName
   * @param ec
   * @return
   */
  def spatialView(docName: String, viewName: String)(implicit ec: ExecutionContext): Future[SpatialView] = {
    Couchbase.spatialView(docName, viewName)(self, ec)
  }

  /**
   *
   *
   *
   * @param docName
   * @param ec
   * @return
   */
  def designDocument(docName: String)(implicit ec: ExecutionContext): Future[DesignDocument] = {
    Couchbase.designDocument(docName)(self, ec)
  }

  /**
   *
   *
   *
   * @param name
   * @param value
   * @param ec
   * @return
   */
  def createDesignDoc(name: String, value: JsObject)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.createDesignDoc(name, value)(self, ec)
  }

  /**
   *
   *
   *
   * @param name
   * @param value
   * @param ec
   * @return
   */
  def createDesignDoc(name: String, value: String)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.createDesignDoc(name, value)(self, ec)
  }

  /**
   *
   *
   *
   * @param value
   * @param ec
   * @return
   */
  def createDesignDoc(value: DesignDocument)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.createDesignDoc(value)(self, ec)
  }

  def deleteDesignDoc(name: String)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.deleteDesignDoc(name)(self, ec)
  }

  /**
   *
   *
   *
   * @param key
   * @param ec
   * @return
   */
  def keyStats(key: String)(implicit ec: ExecutionContext): Future[Map[String, String]] = {
    Couchbase.keyStats(key)(self, ec)
  }

  /**
   *
   *
   *
   * @param key
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def get[T](key: String)(implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    Couchbase.get[T](key)(self, r, ec)
  }

  /**
   *
   *
   *
   * @param key
   * @param ec
   * @return
   */
  def getBlob(key: String)(implicit ec: ExecutionContext): Future[Option[String]] = {
    Couchbase.javaGet(key, self, ec).map {
      case s: String => Some(s)
      case _ => None
    }
  }

  /**
   *
   *
   *
   * @param key
   * @param by
   * @param ec
   * @return
   */
  def incr(key: String, by: Int)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.incr(key, by)(self, ec)

  /**
   *
   *
   *
   * @param key
   * @param by
   * @param ec
   * @return
   */
  def incr(key: String, by: Long)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.incr(key, by)(self, ec)

  /**
   *
   *
   *
   * @param key
   * @param by
   * @param ec
   * @return
   */
  def decr(key: String, by: Int)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.decr(key, by)(self, ec)

  /**
   *
   *
   *
   * @param key
   * @param by
   * @param ec
   * @return
   */
  def decr(key: String, by: Long)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.decr(key, by)(self, ec)

  /**
   *
   *
   *
   * @param key
   * @param by
   * @param ec
   * @return
   */
  def incrAndGet(key: String, by: Int)(implicit ec: ExecutionContext): Future[Int] = Couchbase.incrAndGet(key, by)(self, ec)

  /**
   *
   *
   *
   * @param key
   * @param by
   * @param ec
   * @return
   */
  def incrAndGet(key: String, by: Long)(implicit ec: ExecutionContext): Future[Long] = Couchbase.incrAndGet(key, by)(self, ec)

  /**
   *
   *
   *
   * @param key
   * @param by
   * @param ec
   * @return
   */
  def decrAndGet(key: String, by: Int)(implicit ec: ExecutionContext): Future[Int] = Couchbase.decrAndGet(key, by)(self, ec)

  /**
   *
   *
   *
   * @param key
   * @param by
   * @param ec
   * @return
   */
  def decrAndGet(key: String, by: Long)(implicit ec: ExecutionContext): Future[Long] = Couchbase.decrAndGet(key, by)(self, ec)

  /**
   *
   *
   *
   * @param key
   * @param value
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param ec
   * @return
   */
  def setBlob(key: String, value: String, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.javaSet(key, exp, value, persistTo, replicateTo, self, ec)
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
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def set[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.set[T](key, value, exp, persistTo, replicateTo)(self, w, ec)
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
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def setWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.setWithKey[T](key, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  /**
   *
   *
   *
   * @param value
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def setWithId[T <: { def id: String }](value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.set[T](value.id, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  /**
   *
   *
   *
   * @param data
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def setStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.setStream[T](data, exp, persistTo, replicateTo)(self, w, ec)
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
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def setStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.setStreamWithKey[T](key, data, exp, persistTo, replicateTo)(self, w, ec)
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
   * @param ec
   * @return
   */
  def addBlob(key: String, value: String, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.javaAdd(key, exp, value, persistTo, replicateTo, self, ec)
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
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def add[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.add[T](key, value, exp, persistTo, replicateTo)(self, w, ec)
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
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def addWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.addWithKey[T](key, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  /**
   *
   *
   *
   * @param value
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def addWithId[T <: { def id: String }](value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.add[T](value.id, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  /**
   *
   *
   *
   * @param data
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def addStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.addStream[T](data, exp, persistTo, replicateTo)(self, w, ec)
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
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def addStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.addStreamWithKey[T](key, data, exp, persistTo, replicateTo)(self, w, ec)
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
   * @param ec
   * @return
   */
  def replaceBlob(key: String, value: String, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.javaReplace(key, exp, value, persistTo, replicateTo, self, ec)
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
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def replace[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.replace[T](key, value, exp, persistTo, replicateTo)(self, w, ec)
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
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def replaceWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.replaceWithKey[T](key, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  /**
   *
   *
   *
   * @param value
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def replaceWithId[T <: { def id: String }](value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.replace[T](value.id, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  /**
   *
   *
   *
   * @param data
   * @param exp
   * @param persistTo
   * @param replicateTo
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def replaceStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.replaceStream[T](data, exp, persistTo, replicateTo)(self, w, ec)
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
   * @param w
   * @param ec
   * @tparam T
   * @return
   */
  def replaceStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.replaceStreamWithKey[T](key, data, exp, persistTo, replicateTo)(self, w, ec)
  }

  /**
   *
   *
   *
   * @param key
   * @param persistTo
   * @param replicateTo
   * @param ec
   * @return
   */
  def delete(key: String, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.delete(key, persistTo, replicateTo)(self, ec)
  }

  /**
   *
   *
   *
   * @param value
   * @param persistTo
   * @param replicateTo
   * @param ec
   * @tparam T
   * @return
   */
  def deleteWithId[T <: { def id: String }](value: T, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.delete(value.id, persistTo, replicateTo)(self, ec)
  }

  /**
   *
   *
   *
   * @param data
   * @param persistTo
   * @param replicateTo
   * @param ec
   * @return
   */
  def deleteStream(data: Enumerator[String], persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.deleteStream(data, persistTo, replicateTo)(self, ec)
  }

  /**
   *
   *
   *
   * @param key
   * @param data
   * @param persistTo
   * @param replicateTo
   * @param ec
   * @tparam T
   * @return
   */
  def deleteStreamWithKey[T](key: T => String, data: Enumerator[T], persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.deleteStreamWithKey[T](key, data, persistTo, replicateTo)(self, ec)
  }

  /**
   *
   *
   *
   * @param delay
   * @param ec
   * @return
   */
  def flush(delay: Int)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.flush(delay)(self, ec)

  /**
   *
   *
   *
   * @param ec
   * @return
   */
  def flush()(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.flush()(self, ec)

  /**
   *
   *
   *
   * @param key
   * @param exp
   * @param r
   * @param ec
   * @tparam T
   * @return
   */
  def getAndLock[T](key: String, exp: CouchbaseExpirationTiming)(implicit r: Reads[T], ec: ExecutionContext): Future[Option[CASValue[T]]] = {
    Couchbase.getAndLock(key, exp)(r, self, ec)
  }

  /**
   *
   *
   *
   * @param key
   * @param operation
   * @param ec
   * @param r
   * @param w
   * @tparam T
   * @return
   */
  def atomicUpdate[T](key: String, operation: T => T)(implicit ec: ExecutionContext, r: Reads[T], w: Writes[T]): Future[T] = {
    Couchbase.atomicUpdate[T](key, operation)(self, ec, r, w)
  }

  /**
   *
   *
   *
   * @param key
   * @param operation
   * @param ec
   * @param r
   * @param w
   * @tparam T
   * @return
   */
  def atomicallyUpdate[T](key: String)(operation: T => Future[T])(implicit ec: ExecutionContext, r: Reads[T], w: Writes[T]): Future[T] = {
    Couchbase.atomicallyUpdate[T](key)(operation)(self, ec, r, w)
  }
}
