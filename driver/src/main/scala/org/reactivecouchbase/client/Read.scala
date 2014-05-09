package org.reactivecouchbase.client

import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Future, ExecutionContext}
import org.reactivecouchbase.client.CouchbaseFutures._
import net.spy.memcached.transcoders.Transcoder
import play.api.libs.iteratee.{Enumeratee, Iteratee, Enumerator}
import play.api.libs.json._
import collection.JavaConversions._

/**
 * Trait for read operations
 */
trait Read {

  /**
   *
   * Fetch keys stats
   *
   * @param key the key of the document
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return
   */
  def keyStats(key: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Map[String, String]] = {
    waitForOperation( bucket.couchbaseClient.getKeyStats(key), bucket, ec ).map(_.toMap)
  }

  /**
   *
   * fetch a document
   *
   * @param key the key of the document
   * @param tc the transcoder
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def get[T](key: String, tc: Transcoder[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Option[T]] = {
    waitForGet[T]( bucket.couchbaseClient.asyncGet(key, tc), bucket, ec ) map {
      case value: T => Some[T](value)
      case _ => None
    }
  }

  /**
   *
   * Fetch a stream of documents
   *
   * @param keysEnumerator stream of keys
   * @param bucket the bucket to use
   * @param ec ExecutionContext for async processing
   * @return
   */
  def rawFetch(keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[(String, String)] = {
    QueryEnumerator(() => keysEnumerator.apply(Iteratee.getChunks[String]).flatMap(_.run).flatMap { keys =>
      waitForBulkRaw( bucket.couchbaseClient.asyncGetBulk(keys), bucket, ec ).map { results =>
        Enumerator.enumerate(results.toList)
      }.map { enumerator =>
        enumerator &> Enumeratee.collect[(String, AnyRef)] {
          case (k: String, v: String) => (k, v)
          case (k: String, v: AnyRef) if bucket.failWithNonStringDoc => throw new IllegalStateException(s"Document $k is not a String")
        }
      }
    })
  }

  /**
   *
   * Fetch a stream of documents
   *
   * @param keysEnumerator stream of keys
   * @param bucket the bucket to use
   * @param r Json reader
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def fetch[T](keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = {
    QueryEnumerator(() => rawFetch(keysEnumerator)(bucket, ec).toEnumerator.map { enumerator =>
      enumerator &> Enumeratee.map[(String, String)]( t => (t._1, r.reads(Json.parse(t._2))) ) &> Enumeratee.collect[(String, JsResult[T])] {
        case (k: String, JsSuccess(value, _)) => (k, value)
        case (k: String, JsError(errors)) if bucket.jsonStrictValidation => throw new JsonValidationException("Invalid JSON content", JsError.toFlatJson(errors))
      }
    })
  }

  /**
   *
   * Fetch a stream of documents
   *
   * @param keysEnumerator stream of keys
   * @param bucket the bucket to use
   * @param r Json reader
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def fetchValues[T](keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    QueryEnumerator(() => fetch[T](keysEnumerator)(bucket, r, ec).toEnumerator.map { enumerator =>
      enumerator &> Enumeratee.map[(String, T)](_._2)
    })
  }

  /**
   *
   * Fetch a stream of documents
   *
   * @param keys the key of the documents
   * @param bucket the bucket to use
   * @param r Json reader
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def fetch[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = {
    fetch[T](Enumerator.enumerate(keys))(bucket, r, ec)
  }

  /**
   *
   * Fetch a stream of documents
   *
   * @param keys the key of the documents
   * @param bucket the bucket to use
   * @param r Json reader
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def fetchValues[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    fetchValues[T](Enumerator.enumerate(keys))(bucket, r, ec)
  }

  /**
   *
   * Fetch a optional document
   *
   * @param key the key of the document
   * @param bucket the bucket to use
   * @param r Json reader
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def get[T](key: String)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key), bucket, ec ) map {
      case doc: String => Some(r.reads(Json.parse(doc)))
      case null => None
      case _ if bucket.failWithNonStringDoc => throw new IllegalStateException(s"Document '$key' is not a string ...")
      case _ if !bucket.failWithNonStringDoc => None
    } map {
      case Some(JsSuccess(value, _)) => Some(value)
      case Some(JsError(errors)) if bucket.jsonStrictValidation => throw new JsonValidationException("Invalid JSON content", JsError.toFlatJson(errors))
      case None => None
    }
  }

  /**
   *
   * Fetch a optional document and its key
   *
   * @param key the key of the document
   * @param bucket the bucket to use
   * @param r Json reader
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def getWithKey[T](key: String)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Option[(String, T)]] = {
    get[T](key)(bucket, r, ec).map(_.map( doc => (key, doc)))
  }

  /**
   *
   * Fetch a stream document and their key
   *
   * @param keys the keys of the documents
   * @param bucket the bucket to use
   * @param r Json reader
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def fetchWithKeys[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Map[String, T]] = {
    fetch[T](keys)(bucket, r, ec).toList(ec).map(_.toMap)
  }

  /**
   *
   * Fetch a stream document and their key
   *
   * @param keys the keys of the documents
   * @param bucket the bucket to use
   * @param r Json reader
   * @param ec ExecutionContext for async processing
   * @tparam T type of the doc
   * @return
   */
  def fetchWithKeys[T](keys: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Map[String, T]] = {
    fetch[T](keys)(bucket, r, ec).toList(ec).map(_.toMap)
  }

}
