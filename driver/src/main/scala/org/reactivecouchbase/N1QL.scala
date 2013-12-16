package org.reactivecouchbase.plugins

/*
class CouchbaseN1QLPlugin(app: Application) extends Plugin {

  var queryBase: Option[WSRequestHolder] = None

  override def onStart {
    val host = Configuration.getString("couchbase.n1ql.host").getOrElse(throw new ReactiveCouchbaseException("Cannot find N1QL host", "Cannot find N1QL host in couchbase.n1ql conf."))
    val port = Configuration.getString("couchbase.n1ql.port").getOrElse(throw new ReactiveCouchbaseException("Cannot find N1QL port", "Cannot find N1QL port in couchbase.n1ql conf."))
    queryBase = Some(WS.url(s"http://${host}:${port}/query"))
  }
}

class N1QLQuery(query: String, base: WSRequestHolder) {
  def enumerateJson(implicit ec: ExecutionContext): Future[Enumerator[JsObject]] = {
    toJsArray(ec).map { arr =>
      Enumerator.enumerate(arr.value) &> Enumeratee.map[JsValue](_.as[JsObject])
    }
  }

  def enumerate[T](implicit r: Reads[T], ec: ExecutionContext): Future[Enumerator[T]] = {
    enumerateJson(ec).map { e =>
      e &> Enumeratee.map[JsObject](r.reads(_)) &> Enumeratee.collect[JsResult[T]] { case JsSuccess(value, _) => value }
    }
  }

  def asJsonEnumerator(implicit ec: ExecutionContext): Enumerator[JsObject] = {
    Concurrent.unicast[JsObject](onStart = c => enumerateJson(ec).map(_(Iteratee.foreach[JsObject](c.push).map(_ => c.eofAndEnd()))))
  }

  def asEnumerator[T](implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Concurrent.unicast[T](onStart = c => enumerate[T](r, ec).map(_(Iteratee.foreach[T](c.push).map(_ => c.eofAndEnd()))))
  }

  def toJsArray(implicit ec: ExecutionContext): Future[JsArray] = {
    base.post(Map("q" -> Seq(query))).map { response =>
      (response.json \ "resultset").as[JsArray]
    }
  }

  def toList[T](implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    enumerate[T](r, ec).flatMap(_(Iteratee.getChunks[T]).flatMap(_.run))
  }

  def headOption[T](implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    enumerate[T](r, ec).flatMap(_(Iteratee.head[T]).flatMap(_.run))
  }
}

*/

