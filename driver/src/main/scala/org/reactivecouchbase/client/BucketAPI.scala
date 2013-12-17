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


trait BucketAPI {
  self: CouchbaseBucket =>

  def find[T](docName: String, viewName: String)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    Couchbase.find[T](docName, viewName)(query)(self, r, ec)
  }

  def find[T](view: View)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    Couchbase.find[T](view)(query)(self, r, ec)
  }

  def pollQuery[T](doc: String, view: String, query: Query, everyMillis: Long, filter: T => Boolean = { chunk: T => true })(implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Couchbase.pollQuery[T](doc, view, query, everyMillis, filter)(self, r, ec)
  }

  def repeatQuery[T](doc: String, view: String, query: Query, filter: T => Boolean = { chunk: T => true }, trigger: Future[AnyRef] = Future.successful(Some))(implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Couchbase.repeatQuery[T](doc, view, query, trigger, filter)(self, r, ec)
  }

  def tailableQuery[T](doc: String, view: String, extractor: T => Long, from: Long = 0L, every: Long = 1000L, unit: TimeUnit = TimeUnit.MILLISECONDS)(implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Couchbase.tailableQuery[T](doc, view, extractor, from, every, unit)(self, r, ec)
  }

  def rawFetch(keysEnumerator: Enumerator[String])(implicit ec: ExecutionContext): QueryEnumerator[(String, String)] = Couchbase.rawFetch(keysEnumerator)(this, ec)
  def fetch[T](keysEnumerator: Enumerator[String])(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = Couchbase.fetch[T](keysEnumerator)(this, r, ec)
  def fetchValues[T](keysEnumerator: Enumerator[String])(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = Couchbase.fetchValues[T](keysEnumerator)(this, r, ec)
  def fetch[T](keys: Seq[String])(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = Couchbase.fetch[T](keys)(this, r, ec)
  def fetchValues[T](keys: Seq[String])(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = Couchbase.fetchValues[T](keys)(this, r, ec)
  def getWithKey[T](key: String)(implicit r: Reads[T], ec: ExecutionContext): Future[Option[(String, T)]] = Couchbase.getWithKey[T](key)(this, r, ec)
  def rawSearch(docName: String, viewName: String)(query: Query)(implicit ec: ExecutionContext): QueryEnumerator[RawRow] = Couchbase.rawSearch(docName, viewName)(query)(this, ec)
  def rawSearch(view: View)(query: Query)(implicit ec: ExecutionContext): QueryEnumerator[RawRow] = Couchbase.rawSearch(view)(query)(this, ec)
  def search[T](docName: String, viewName: String)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = Couchbase.search[T](docName, viewName)(query)(this, r, ec)
  def search[T](view: View)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = Couchbase.search[T](view)(query)(this, r, ec)
  def searchValues[T](docName: String, viewName: String)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = Couchbase.searchValues[T](docName, viewName)(query)(this, r, ec)
  def searchValues[T](view: View)(query: Query)(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = Couchbase.searchValues[T](view)(query)(this, r, ec)

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def view(docName: String, viewName: String)(implicit ec: ExecutionContext): Future[View] = {
    Couchbase.view(docName, viewName)(self, ec)
  }

  def spatialView(docName: String, viewName: String)(implicit ec: ExecutionContext): Future[SpatialView] = {
    Couchbase.spatialView(docName, viewName)(self, ec)
  }

  def designDocument(docName: String)(implicit ec: ExecutionContext): Future[DesignDocument] = {
    Couchbase.designDocument(docName)(self, ec)
  }

  def createDesignDoc(name: String, value: JsObject)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.createDesignDoc(name, value)(self, ec)
  }

  def createDesignDoc(name: String, value: String)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.createDesignDoc(name, value)(self, ec)
  }

  def createDesignDoc(value: DesignDocument)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.createDesignDoc(value)(self, ec)
  }

  def deleteDesignDoc(name: String)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.deleteDesignDoc(name)(self, ec)
  }

  def keyStats(key: String)(implicit ec: ExecutionContext): Future[Map[String, String]] = {
    Couchbase.keyStats(key)(self, ec)
  }

  def get[T](key: String)(implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    Couchbase.get[T](key)(self, r, ec)
  }

  def getBlob(key: String)(implicit ec: ExecutionContext): Future[Option[String]] = {
    Couchbase.javaGet(key, self, ec).map {
      case s: String => Some(s)
      case _ => None
    }
  }

  def incr(key: String, by: Int)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.incr(key, by)(self, ec)
  def incr(key: String, by: Long)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.incr(key, by)(self, ec)
  def decr(key: String, by: Int)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.decr(key, by)(self, ec)
  def decr(key: String, by: Long)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.decr(key, by)(self, ec)
  def incrAndGet(key: String, by: Int)(implicit ec: ExecutionContext): Future[Int] = Couchbase.incrAndGet(key, by)(self, ec)
  def incrAndGet(key: String, by: Long)(implicit ec: ExecutionContext): Future[Long] = Couchbase.incrAndGet(key, by)(self, ec)
  def decrAndGet(key: String, by: Int)(implicit ec: ExecutionContext): Future[Int] = Couchbase.decrAndGet(key, by)(self, ec)
  def decrAndGet(key: String, by: Long)(implicit ec: ExecutionContext): Future[Long] = Couchbase.decrAndGet(key, by)(self, ec)

  def setBlob(key: String, value: String, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.javaSet(key, exp, value, persistTo, replicateTo, self, ec)
  }

  def set[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.set[T](key, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  def setWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.setWithKey[T](key, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  def setWithId[T <: { def id: String }](value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.set[T](value.id, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  def setStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.setStream[T](data, exp, persistTo, replicateTo)(self, w, ec)
  }

  def setStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.setStreamWithKey[T](key, data, exp, persistTo, replicateTo)(self, w, ec)
  }

  def addBlob(key: String, value: String, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.javaAdd(key, exp, value, persistTo, replicateTo, self, ec)
  }

  def add[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.add[T](key, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  def addWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.addWithKey[T](key, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  def addWithId[T <: { def id: String }](value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.add[T](value.id, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  def addStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.addStream[T](data, exp, persistTo, replicateTo)(self, w, ec)
  }

  def addStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.addStreamWithKey[T](key, data, exp, persistTo, replicateTo)(self, w, ec)
  }

  def replaceBlob(key: String, value: String, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.javaReplace(key, exp, value, persistTo, replicateTo, self, ec)
  }

  def replace[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.replace[T](key, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  def replaceWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.replaceWithKey[T](key, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  def replaceWithId[T <: { def id: String }](value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.replace[T](value.id, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  def replaceStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.replaceStream[T](data, exp, persistTo, replicateTo)(self, w, ec)
  }

  def replaceStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.replaceStreamWithKey[T](key, data, exp, persistTo, replicateTo)(self, w, ec)
  }

  def delete(key: String, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.delete(key, persistTo, replicateTo)(self, ec)
  }

  def deleteWithId[T <: { def id: String }](value: T, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[OperationStatus] = {
    Couchbase.delete(value.id, persistTo, replicateTo)(self, ec)
  }

  def deleteStream(data: Enumerator[String], persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.deleteStream(data, persistTo, replicateTo)(self, ec)
  }

  def deleteStreamWithKey[T](key: T => String, data: Enumerator[T], persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[List[OperationStatus]] = {
    Couchbase.deleteStreamWithKey[T](key, data, persistTo, replicateTo)(self, ec)
  }

  def flush(delay: Int)(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.flush(delay)(self, ec)
  def flush()(implicit ec: ExecutionContext): Future[OperationStatus] = Couchbase.flush()(self, ec)

  def getAndLock[T](key: String, exp: CouchbaseExpirationTiming)(implicit r: Reads[T], ec: ExecutionContext): Future[Option[CASValue[T]]] = {
    Couchbase.getAndLock(key, exp)(r, self, ec)
  }
  
  def atomicUpdate[T](key: String, operation: T => T)(implicit ec: ExecutionContext, r: Reads[T],  w: Writes[T]):Future[Any] = {
    Couchbase.atomicUpdate[T](key, operation)(self, ec, r, w)
  }
}
