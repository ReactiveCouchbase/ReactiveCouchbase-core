package org.reactivecouchbase.experimental

import org.reactivecouchbase._
import com.couchbase.client.protocol.views.{ComplexKey, Stale, Query}
import play.api.libs.json._
import scala.concurrent.{Await, Future, ExecutionContext}
import java.util.concurrent.{ConcurrentLinkedQueue, ConcurrentHashMap, TimeUnit}
import play.api.libs.iteratee.{Enumeratee, Enumerator}
import net.spy.memcached.{ReplicateTo, PersistTo}
import scala.concurrent.duration.Duration
import org.reactivecouchbase.CouchbaseExpiration._
import org.reactivecouchbase.client.{OpResult, Constants}
import scala.Some
import play.api.libs.json.JsObject

object CappedBucket {
  //private[reactivecouchbase] val buckets = new ConcurrentHashMap[String, CappedBucket]()
  private def docName = "play2couchbase-cappedbucket-designdoc"
  private def viewName = "byNaturalOrder"
  private def cappedRef = "__playcbcapped"
  private def cappedNaturalId = "__playcbcappednatural"
  //private def designDocOld =
  //  s"""
  //    {
  //      "views":{
  //         "byNaturalOrder": {
  //             "map": "function (doc, meta) { if (doc.$cappedRef) { if (doc.$cappedNaturalId) { emit(doc.$cappedNaturalId, null); } } } "
  //         }
  //      }
  //    }
  //  """

  private def designDoc = Json.obj(
    "views" -> Json.obj(
      "byNaturalOrder" -> Json.obj(
        "map" -> s"""
                    | function (doc, meta) {
                    |   if (doc.$cappedRef) {
                    |     if (doc.$cappedNaturalId) {
                    |       emit(doc.$cappedNaturalId, null);
                    |     }
                    |   }
                    | }
                  """.stripMargin
      )
    )
  )

  def clearCache() = {
    //buckets.clear()
    //reaperOn.foreach( entry => entry._2.cancel() )
    //reaperOn.clear()
  }

  def clear(name: String) = {
    //buckets.remove(name)
    //if (reaperOn.containsKey(name)) reaperOn.get(name).cancel()
    //reaperOn.remove(name)
  }

  /**
   *
   * Build a Capped Bucket from a bucket
   *
   * @param bucket the bucket to use the bucket to transform in capped bucket
   * @param ec ExecutionContext for async processing ExecutionContext for async processing
   * @param max max elements in the capped bucket
   * @param reaper trigger reaper to kill elements after max
   * @return the capped bucket
   */
  def apply(bucket: () => CouchbaseBucket, ec: ExecutionContext, max: Int = 500, reaper: Boolean = false) = {
    //if (!buckets.containsKey(bucket.alias)) {
    //  buckets.putIfAbsent(bucket.alias, new CappedBucket(bucket, ec, max, reaper))
    //}
    //buckets.get(bucket.alias)
    new CappedBucket(bucket, ec, max, reaper)
  }

  /*private def enabledReaper(bucket: CouchbaseBucket, max: Int, ec: ExecutionContext) = {
    if (!reaperOn.containsKey(bucket.alias)) {
      bucket.driver.logger.info(s"Capped reaper is on for ${bucket.alias} ...")
      val cancel = bucket.driver.scheduler().schedule(Duration(0, TimeUnit.MILLISECONDS), Duration(1000, TimeUnit.MILLISECONDS))({
        if (reaperOn.containsKey(bucket.alias)) {
          val query = new Query().setIncludeDocs(false).setStale(Stale.FALSE).setDescending(true).setSkip(max)
          bucket.rawSearch(docName, viewName)(query)(ec).toList(ec).map { f =>
            f.map { elem => bucket.delete(elem.id.get)(ec) }
          }(ec)
        }
      })(ec)
      reaperOn.putIfAbsent(bucket.alias, cancel)
    }
  }*/

  //private[reactivecouchbase] lazy val reaperOn = new ConcurrentHashMap[String, Cancellable]()
  //private lazy val triggerPromise = Promise[Unit]()
  //private lazy val trigger = triggerPromise.future
  private[reactivecouchbase] lazy val views = new ConcurrentHashMap[String, Boolean]()

  private[reactivecouchbase] def trigger(bucket: CouchbaseBucket): Future[Boolean] = {
    if (!CappedBucket.views.containsKey(bucket.alias)) {
      Future.failed(new Throwable("CappedBucket not ready yet"))
    } else {
      Future.successful(true)//CappedBucket.views.get(bucket.alias)
    }
  }

  private def setupViews(bucket: CouchbaseBucket, ec: ExecutionContext) = {
    //bucket.createDesignDoc(CappedBucket.docName, CappedBucket.designDoc)(ec).map(_ => triggerPromise.trySuccess(()))(ec)
    bucket.createDesignDoc(CappedBucket.docName, CappedBucket.designDoc)(ec)//.map(_ => CappedBucket.views.put(bucket.alias, Future.successful(())))(ec)
  }
}

/**
 *
 * Represent a Capped bucket (capped is handle in the driver, not on the server side)
 *
 * @param bucket the bucket to use the bucket to use
 * @param ec ExecutionContext for async processing 
 * @param max max elements in the bucket
 * @param reaper enable reaper to remove old elements
 */
class CappedBucket(bucket: () => CouchbaseBucket, ec: ExecutionContext, max: Int, reaper: Boolean = false) {

  //if (!CappedBucket.triggerPromise.isCompleted) CappedBucket.setupViews(bucket, ec)
  //if (reaper) CappedBucket.enabledReaper(bucket, max, ec)

  if (!CappedBucket.views.containsKey(bucket().alias)) {
    Await.result(CappedBucket.setupViews(bucket(), ec), Duration(10, TimeUnit.SECONDS)) // I know that's ugly
    CappedBucket.views.put(bucket().alias, true)
  }

  /**
   *
   * Retrieve the oldest document from the capped bucket
   *
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def oldestOption[T](implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    val query = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setDescending(false).setLimit(1)
    CappedBucket.trigger(bucket()).flatMap(_ => Couchbase.find[T](CappedBucket.docName, CappedBucket.viewName)(query)(bucket(), r, ec).map(_.headOption))
  }

  /**
   *
   * Retrieve the last inserted document from the capped bucket
   *
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def lastInsertedOption[T](implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    val query = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setDescending(true).setLimit(1)
    CappedBucket.trigger(bucket()).flatMap(_ => Couchbase.find[T](CappedBucket.docName, CappedBucket.viewName)(query)(bucket(), r, ec).map(_.headOption))
  }

  /**
   *
   * Retrieve an infinite stream of data from a capped bucket. Each time a document is inserted in the bucket, doc
   * document is pushed in the stream.
   *
   * @param from natural insertion id to retrieve from
   * @param every retrieve new doc every, default is 500 ms
   * @param unit time unit
   * @param r Json reader for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def tail[T](from: Long = 0L, every: Long = 200, unit: TimeUnit = TimeUnit.MILLISECONDS)(implicit r: Reads[T], ec: ExecutionContext): Enumerator[T]= {
    Enumerator.flatten(CappedBucket.trigger(bucket()).map( _ => Couchbase.tailableQuery[JsObject](CappedBucket.docName, CappedBucket.viewName, { obj =>
      (obj \ CappedBucket.cappedNaturalId).as[Long]
    }, from, every, unit)(bucket, CouchbaseRWImplicits.documentAsJsObjectReader, ec).through(Enumeratee.map { elem =>
       r.reads(elem.asInstanceOf[JsValue])
    }).through(Enumeratee.collect {
      case JsSuccess(elem, _) => elem
    })))
  }

  /**
   *
   * Insert document into the capped bucket
   *
   * @param key the key of the inserted doc
   * @param value the doc to insert
   * @param exp expiration of data
   * @param persistTo persistance flag
   * @param replicateTo replication flag
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def insert[T](key: String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext):  Future[OpResult] = {
    val jsObj = w.writes(value).as[JsObject]
    val enhancedJsObj = jsObj ++ Json.obj(CappedBucket.cappedRef -> true, CappedBucket.cappedNaturalId -> System.currentTimeMillis())
    CappedBucket.trigger(bucket()).flatMap(_ => Couchbase.set[JsObject](key, enhancedJsObj, exp, persistTo, replicateTo)(bucket(), CouchbaseRWImplicits.jsObjectToDocumentWriter, ec))
  }

  /**
   *
   * Insert a document into the capped bucket
   *
   * @param key the key of the inserted doc
   * @param value the doc to insert
   * @param exp expiration of data
   * @param persistTo persistance flag
   * @param replicateTo replication flag
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def insertWithKey[T](key: T => String, value: T, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext):  Future[OpResult] = {
    val jsObj = w.writes(value).as[JsObject]
    val enhancedJsObj = jsObj ++ Json.obj(CappedBucket.cappedRef -> true, CappedBucket.cappedNaturalId -> System.currentTimeMillis())
    CappedBucket.trigger(bucket()).flatMap(_ => Couchbase.setWithKey[JsObject]({ _ => key(value)}, enhancedJsObj, exp, persistTo, replicateTo)(bucket(), CouchbaseRWImplicits.jsObjectToDocumentWriter, ec))
  }

  /**
   *
   * Insert a stream of data into the capped bucket
   *
   * @param data stream of data
   * @param exp expiration of data to insert
   * @param persistTo persistance flag
   * @param replicateTo replication flag
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def insertStream[T](data: Enumerator[(String, T)], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OpResult]] = {
    val enhancedEnumerator = data.through(Enumeratee.map { elem =>
      val jsObj = w.writes(elem._2).as[JsObject]
      val enhancedJsObj = jsObj ++ Json.obj(CappedBucket.cappedRef -> true, CappedBucket.cappedNaturalId -> System.currentTimeMillis())
      (elem._1, enhancedJsObj)
    })
    CappedBucket.trigger(bucket()).flatMap(_ => Couchbase.setStream[JsObject](enhancedEnumerator, exp, persistTo, replicateTo)(bucket(), CouchbaseRWImplicits.jsObjectToDocumentWriter, ec))
  }

  /**
   *
   * Insert a stream of data into the capped bucket
   *
   * @param key the key extractor
   * @param data stream of data to insert
   * @param exp expiration of data
   * @param persistTo persistance flag
   * @param replicateTo replication flag
   * @param w Json writer for type T
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def insertStreamWithKey[T](key: T => String, data: Enumerator[T], exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.ZERO, replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit w: Writes[T], ec: ExecutionContext): Future[List[OpResult]] = {
    val enhancedEnumerator = data.through(Enumeratee.map { elem =>
      val jsObj = w.writes(elem).as[JsObject]
      val enhancedJsObj = jsObj ++ Json.obj(CappedBucket.cappedRef -> true, CappedBucket.cappedNaturalId -> System.currentTimeMillis())
      (key(elem), enhancedJsObj)
    })
    CappedBucket.trigger(bucket()).flatMap(_ => Couchbase.setStream[JsObject](enhancedEnumerator, exp, persistTo, replicateTo)(bucket(), CouchbaseRWImplicits.jsObjectToDocumentWriter, ec))
  }
}
