package org.reactivecouchbase

import com.couchbase.client.{ CouchbaseConnectionFactoryBuilder, CouchbaseClient }
import java.net.URI
import java.util.concurrent.{ AbstractExecutorService, TimeUnit }
import collection.JavaConversions._
import collection.mutable.ArrayBuffer
import scala.Some
import scala.concurrent.{ ExecutionContextExecutorService, ExecutionContext }
import akka.actor.ActorSystem
import java.util.Collections
import org.reactivecouchbase.client._

class CouchbaseBucket(val client: Option[CouchbaseClient], val hosts: List[String], val port: String, val base: String, val bucket: String, val user: String, val pass: String, val timeout: Long, ec: ExecutionContext) extends BucketAPI {

  def connect() = {
    val uris = ArrayBuffer(hosts.map { h => URI.create(s"http://$h:$port/$base") }: _*)
    val cfb = new CouchbaseConnectionFactoryBuilder()
    if (Configuration.getBoolean("couchbase.driver.useec").getOrElse(true)) {
      cfb.setListenerExecutorService(ExecutionContextExecutorServiceBridge.apply(ec))
    }
    val cf = cfb.buildCouchbaseConnection(uris, bucket, user, pass);
    val client = new CouchbaseClient(cf);
    new CouchbaseBucket(Some(client), hosts, port, base, bucket, user, pass, timeout, ec)
  }

  def disconnect() = {
    client.map(_.shutdown(timeout, TimeUnit.SECONDS))
    new CouchbaseBucket(None, hosts, port, base, bucket, user, pass, timeout, ec)
  }

  def couchbaseClient: CouchbaseClient = {
    client.getOrElse(throw new ReactiveCouchbaseException(s"Error with bucket ${bucket}", s"Bucket '${bucket}' is not defined or client is not connected"))
  }

  def executionContext = ec
}

object Couchbase extends Read with Write with Delete with Counters with Queries with JavaApi with Atomic {

  def apply(
    hosts: List[String] = List(Configuration.getString("couchbase.bucket.host").getOrElse("127.0.0.1")),
    port: String = Configuration.getString("couchbase.bucket.port").getOrElse("8091"),
    base: String = Configuration.getString("couchbase.bucket.base").getOrElse("pools"),
    bucket: String = Configuration.getString("couchbase.bucket.bucket").getOrElse("default"),
    user: String = Configuration.getString("couchbase.bucket.user").getOrElse(""),
    pass: String = Configuration.getString("couchbase.bucket.pass").getOrElse(""),
    timeout: Long = Configuration.getLong("couchbase.bucket.timeout").getOrElse(0),
    ec: ExecutionContext): CouchbaseBucket = {
    new CouchbaseBucket(None, hosts, port, base, bucket, user, pass, timeout, ec)
  }
}

object ExecutionContextExecutorServiceBridge {
  def apply(ec: ExecutionContext): ExecutionContextExecutorService = ec match {
    case null => throw new Throwable("ExecutionContext to ExecutorService conversion failed !!!")
    case eces: ExecutionContextExecutorService => eces
    case other => new AbstractExecutorService with ExecutionContextExecutorService {
      override def prepare(): ExecutionContext = other
      override def isShutdown = false
      override def isTerminated = false
      override def shutdown() = ()
      override def shutdownNow() = Collections.emptyList[Runnable]
      override def execute(runnable: Runnable): Unit = other execute runnable
      override def reportFailure(t: Throwable): Unit = other reportFailure t
      override def awaitTermination(length: Long, unit: TimeUnit): Boolean = false
    }
  }
}
