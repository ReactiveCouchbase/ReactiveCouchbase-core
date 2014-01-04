package org.reactivecouchbase.experimental

import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig}
import com.couchbase.client.protocol.views.{Query, View}
import org.reactivecouchbase.CouchbaseBucket
import scala.concurrent.ExecutionContext

object Views {

  private val config: AsyncHttpClientConfig = new AsyncHttpClientConfig.Builder()
    .setAllowPoolingConnection(true)
    .setCompressionEnabled(true)
    .setRequestTimeoutInMs(600000)
    .setIdleConnectionInPoolTimeoutInMs(600000)
    .setIdleConnectionTimeoutInMs(600000)
    .build()

  private[reactivecouchbase] val client: AsyncHttpClient = new AsyncHttpClient(config)

  def query(view: View, query: Query)(implicit bucket: CouchbaseBucket, ec: ExecutionContext) = {
    view.getURI
  }

}
