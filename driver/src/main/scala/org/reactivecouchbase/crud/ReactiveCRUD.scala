package org.reactivecouchbase.crud

import play.api.libs.json._
import org.reactivecouchbase.{CouchbaseRWImplicits, CouchbaseBucket}
import scala.concurrent.{Future, ExecutionContext}
import java.util.UUID
import com.couchbase.client.protocol.views.{Query, View}
import play.api.libs.iteratee.{Enumeratee, Iteratee, Enumerator}
import org.reactivecouchbase.client.{OpResult, ReactiveCouchbaseException, TypedRow}

/**
 *
 * Util trait to avoid implementing basic CRUD operations
 *
 * @tparam T type of the doc
 */
abstract class ReactiveCRUD[T](implicit fmt: Format[T], ctx: ExecutionContext) {

  import org.reactivecouchbase.CouchbaseRWImplicits._

  /**
   * @return the bucket used
   */
  def bucket: CouchbaseBucket

  /** Override to customize deserialization and add validation. */
  protected val reader: Reads[T]  = fmt

  /** Override to customize serialization. */
  protected val writer: Writes[T] = fmt

  /**
   * @return field name of the doc used as Id
   */
  def idKey: String = "_id"

  def generateId() = UUID.randomUUID().toString

  /**
   *
   * Insert a new document in the bucket
   *
   * @param t
   * @return
   */
  def insert(t: T): Future[(String, OpResult)] = {
    val id: String = generateId()
    val json = writer.writes(t).as[JsObject]
    json \ idKey match {
      case _: JsUndefined => {
        val newJson = json ++ Json.obj(idKey -> JsString(id))
        bucket.set(id, newJson)(CouchbaseRWImplicits.jsObjectToDocumentWriter, ctx).map(r => (id, r))(ctx)
      }
      case actualId: JsString => {
        bucket.set(actualId.value, json)(CouchbaseRWImplicits.jsObjectToDocumentWriter, ctx).map(r => (actualId.value, r))(ctx)
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
    bucket.get[T]( id )(reader, ctx).map( _.map( v => ( v, id ) ) )(ctx)
  }

  /**
   *
   * Delete a document
   *
   * @param id key of the document
   * @return nothing useful
   */
  def delete(id: String): Future[OpResult] = {
    bucket.delete(id)(ctx)
  }

  /**
   *
   * Update a document
   *
   * @param id key of the document
   * @param t the new value for the key
   * @return  nothing useful
   */
  def update(id: String, t: T): Future[OpResult] = {
    bucket.replace(id, t)(writer, ctx)
  }

  /**
   *
   * Partially update the content of a document
   *
   * @param id the key of the document
   * @param upd the partial update json object
   * @return nothing useful
   */
  def updatePartial(id: String, upd: JsObject): Future[OpResult] = {
    get(id).flatMap { opt =>
      opt.map { t =>
        val json = Json.toJson(t._1)(writer).as[JsObject]
        val newJson = json.deepMerge(upd)
        bucket.replace((json \ idKey).as[JsString].value, newJson)(CouchbaseRWImplicits.jsObjectToDocumentWriter, ctx)
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
  def batchInsert(elems: Enumerator[T]): Future[List[OpResult]] = {
    //elems(Iteratee.foldM[T, Int](0)( (s, t) => insert(t).map(_ => s + 1))).flatMap(_.run)
    elems(Iteratee.foldM[T, List[OpResult]](List[OpResult]())( (s, t) => insert(t).map(st => s :+ st._2))).flatMap(_.run)
  }

  /**
   *
   * Search for documents
   *
   * @param view the view
   * @param q the query
   * @param limit the size of the returned collection
   * @param skip to skip some documents
   * @return sequence of results
   */
  def find(view: View, q: Query, limit: Int = 0, skip: Int = 0): Future[Seq[(T, String)]] = {
    var query = q
    if (limit != 0) query = query.setLimit(limit)
    if (skip != 0) query = query.setSkip(skip)
    bucket.search[JsObject](view)(query)(CouchbaseRWImplicits.documentAsJsObjectReader, ctx).toList(ctx).map { l =>
      l.map { i =>
        val t = reader.reads(i.document) match {
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
   * @param view the view
   * @param q the query
   * @param pageSize the size of the returned collection
   * @param skip to skip some documents
   * @return stream of results
   */
  def findStream(view: View, q: Query, skip: Int = 0, pageSize: Int = 0): Enumerator[Iterator[(T, String)]] = {
    var query = q
    if (skip != 0) query = query.setSkip(skip)
    val futureEnumerator = bucket.search[JsObject](view)(query)(CouchbaseRWImplicits.documentAsJsObjectReader, ctx).toList(ctx).map { l =>
      val size = if(pageSize != 0) pageSize else l.size
      Enumerator.enumerate(l.map { i =>
        val t = reader.reads(i.document) match {
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
   * @param view the view
   * @param query the query to find doc to delete
   * @return nothing useful
   */
  def batchDelete(view: View, query: Query): Future[List[OpResult]] = {
    val extract = { tr: TypedRow[JsObject] => tr.id.get }
    bucket.search[JsObject](view)(query)(CouchbaseRWImplicits.documentAsJsObjectReader, ctx).toEnumerator.flatMap { enumerator =>
      bucket.deleteStreamWithKey[TypedRow[JsObject]](extract, enumerator)(ctx)
    }
  }

  /**
   *
   * Update multiple documents
   *
   * @param view the view
   * @param query the query to fond doc to update
   * @param upd partial update of documents
   * @return
   */
  def batchUpdate(view: View, query: Query, upd: JsObject): Future[List[OpResult]] = {
    bucket.search[T](view)(query)(reader, ctx).toEnumerator.flatMap { enumerator =>
      bucket.replaceStream(enumerator.through(Enumeratee.map { t =>
        val json = Json.toJson(t.document)(writer).as[JsObject]
        (t.id.get, json.deepMerge(upd))
      }))(CouchbaseRWImplicits.jsObjectToDocumentWriter, ctx)
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