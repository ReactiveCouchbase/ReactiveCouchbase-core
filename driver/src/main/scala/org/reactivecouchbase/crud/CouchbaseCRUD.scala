package org.reactivecouchbase.crud

import play.api.libs.json._
import org.reactivecouchbase.{CouchbaseRWImplicits, CouchbaseBucket}
import scala.concurrent.{Future, ExecutionContext}
import java.util.UUID
import com.couchbase.client.protocol.views.{Query, View}
import play.api.libs.iteratee.{Enumeratee, Iteratee, Enumerator}
import org.reactivecouchbase.client.{ReactiveCouchbaseException, TypedRow}

/**
 *
 * Util trait to avoid implementing basic CRUD operations
 *
 * @tparam T type of the doc
 */
trait ReactiveCRUD[T] {

  import org.reactivecouchbase.CouchbaseRWImplicits._

  /**
   * @return ExecutionContext for async processing
   */
  implicit def ctx: ExecutionContext// = implicitly[ExecutionContext]

  /**
   * @return the bucket used
   */
  def bucket: CouchbaseBucket

  /**
   * @return the JSON format for T
   */
  def format: Format[T]

  /**
   * @return field name of the doc used as Id
   */
  def idKey: String = "_id"

  /**
   *
   * Insert a new document in the bucket
   *
   * @param t
   * @return
   */
  def insert(t: T): Future[String] = {
    val id: String = UUID.randomUUID().toString
    val json = format.writes(t).as[JsObject]
    json \ idKey match {
      case _: JsUndefined => {
        val newJson = json ++ Json.obj(idKey -> JsString(id))
        bucket.set(id, newJson)(CouchbaseRWImplicits.jsObjectToDocumentWriter, ctx).map(_ => id)(ctx)
      }
      case actualId: JsString => {
        bucket.set(actualId.value, json)(CouchbaseRWImplicits.jsObjectToDocumentWriter, ctx).map(_ => actualId.value)(ctx)
      }
      case _ => throw new ReactiveCouchbaseException("Error", s"Field with $idKey already exists and not of type JsString")
    }
  }

  /**
   *
   * Fetch a document
   *
   * @param id the key of the document
   * @return maybe the document and its key if found
   */
  def get(id: String): Future[Option[(T, String)]] = {
    bucket.get[T]( id )(format, ctx).map( _.map( v => ( v, id ) ) )(ctx)
  }

  /**
   *
   * Delete a document
   *
   * @param id key of the document
   * @return nothing useful
   */
  def delete(id: String): Future[Unit] = {
    bucket.delete(id)(ctx).map(_ => ())
  }

  /**
   *
   * Update a document
   *
   * @param id key of the document
   * @param t the new value for the key
   * @return  nothing useful
   */
  def update(id: String, t: T): Future[Unit] = {
    bucket.replace(id, t)(format, ctx).map(_ => ())
  }

  /**
   *
   * Partially update the content of a document
   *
   * @param id the key of the document
   * @param upd the partial update json object
   * @return nothing useful
   */
  def updatePartial(id: String, upd: JsObject): Future[Unit] = {
    get(id).flatMap { opt =>
      opt.map { t =>
        val json = Json.toJson(t._1)(format).as[JsObject]
        val newJson = json.deepMerge(upd)
        bucket.replace((json \ idKey).as[JsString].value, newJson)(CouchbaseRWImplicits.jsObjectToDocumentWriter, ctx).map(_ => ())
      }.getOrElse(throw new ReactiveCouchbaseException("Error", s"Cannot find id $id"))
    }
  }

  /**
   *
   * Insert a stream of documents
   *
   * @param elems the stream of documents
   * @return the number of documents inserted
   */
  def batchInsert(elems: Enumerator[T]): Future[Int] = {
    elems(Iteratee.foldM[T, Int](0)( (s, t) => insert(t).map(_ => s + 1))).flatMap(_.run)
  }

  /**
   *
   * Search for documents
   *
   * @param sel the query
   * @param limit the size of the returned collection
   * @param skip to skip some documents
   * @return sequence of results
   */
  def find(sel: (View, Query), limit: Int = 0, skip: Int = 0): Future[Seq[(T, String)]] = {
    var query = sel._2
    if (limit != 0) query = query.setLimit(limit)
    if (skip != 0) query = query.setSkip(skip)
    bucket.search[JsObject](sel._1)(query)(CouchbaseRWImplicits.documentAsJsObjectReader, ctx).toList(ctx).map { l =>
      l.map { i =>
        val t = format.reads(i.document) match {
          case e:JsError => throw new ReactiveCouchbaseException("Error", "Document does not match object")
          case s:JsSuccess[T] => s.get
        }
        i.document \ idKey match {
          case actualId: JsString => (t, actualId.value)
          case _ => (t, i.id.get)
        }
      }
    }
  }

  /**
   *
   * Search for documents (result as stream)
   *
   * @param sel the query
   * @param limit the size of the returned collection
   * @param skip to skip some documents
   * @return stream of results
   */
  def findStream(sel: (View, Query), skip: Int = 0, pageSize: Int = 0): Enumerator[Iterator[(T, String)]] = {
    var query = sel._2
    if (skip != 0) query = query.setSkip(skip)
    val futureEnumerator = bucket.search[JsObject](sel._1)(query)(CouchbaseRWImplicits.documentAsJsObjectReader, ctx).toList(ctx).map { l =>
      val size = if(pageSize != 0) pageSize else l.size
      Enumerator.enumerate(l.map { i =>
        val t = format.reads(i.document) match {
          case e:JsError => throw new ReactiveCouchbaseException("Error", "Document does not match object")
          case s:JsSuccess[T] => s.get
        }
        i.document \ idKey match {
          case actualId: JsString => (t, actualId.value)
          case _ => (t, i.id.get)
        }
      }.grouped(size).map(_.iterator))
    }
    Enumerator.flatten(futureEnumerator)
  }

  /**
   *
   * Delete multiple documents
   *
   * @param sel query to find documents to delete
   * @return nothing useful
   */
  def batchDelete(sel: (View, Query)): Future[Unit] = {
    val extract = { tr: TypedRow[JsObject] => tr.id.get }
    bucket.search[JsObject](sel._1)(sel._2)(CouchbaseRWImplicits.documentAsJsObjectReader, ctx).enumerate.map { enumerator =>
      bucket.deleteStreamWithKey[TypedRow[JsObject]](extract, enumerator)(ctx)
    }.map(_ => ())
  }

  /**
   *
   * Update multiple documents
   *
   * @param sel query to find documents to update
   * @param upd partial update of documents
   * @return
   */
  def batchUpdate(sel: (View, Query), upd: JsObject): Future[Unit] = {
    bucket.search[T](sel._1)(sel._2)(format, ctx).enumerate.map { enumerator =>
      bucket.replaceStream(enumerator.through(Enumeratee.map { t =>
        val json = Json.toJson(t.document)(format).as[JsObject]
        (t.id.get, json.deepMerge(upd))
      }))(CouchbaseRWImplicits.jsObjectToDocumentWriter, ctx).map(_ => ())
    }
  }

  /**
   *
   * Fetch a view
   *
   * @param docName design doc name
   * @param viewName the name of the view to fetch
   * @return the fetched view
   */
  private def view(docName: String, viewName: String): Future[View] = {
    bucket.view(docName, viewName)(ctx)
  }
}