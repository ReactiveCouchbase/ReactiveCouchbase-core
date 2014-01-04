package org.reactivecouchbase.experimental

import com.ning.http.client.{Response, AsyncCompletionHandler, AsyncHttpClient, AsyncHttpClientConfig}
import com.couchbase.client.protocol.views.{Query, View}
import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.{Promise, Future, ExecutionContext}
import play.api.libs.json.{Json, JsArray}

object Views {

  private val config: AsyncHttpClientConfig = new AsyncHttpClientConfig.Builder()
    .setAllowPoolingConnection(true)
    .setCompressionEnabled(true)
    .setRequestTimeoutInMs(600000)
    .setIdleConnectionInPoolTimeoutInMs(600000)
    .setIdleConnectionTimeoutInMs(600000)
    .build()

  private[reactivecouchbase] val client: AsyncHttpClient = new AsyncHttpClient(config)


  /*private[experimental]*/ def query(view: View, query: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[JsArray] = {
    val url = s"http://${bucket.hosts.head}:8092${view.getURI}${query.toString}&include_docs=${query.willIncludeDocs()}}"
    val promise = Promise[String]()
    client.prepareGet(url).execute(new AsyncCompletionHandler[Response]() {
      override def onCompleted(response: Response) = {
        if (response.getStatusCode != 200) {
          promise.failure(new RuntimeException(s"Couchbase responded with status '${response.getStatusCode}' : ${response.getResponseBody}"))
        } else {
          promise.success(response.getResponseBody)
        }
        response
      }
      override def onThrowable(t: Throwable) = {
        promise.failure(t)
      }
    })
    promise.future.map(body => (Json.parse(body) \ "rows").as[JsArray])
  }

}
