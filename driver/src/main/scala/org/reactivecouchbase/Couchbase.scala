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

class CouchbaseBucket(val client: Option[CouchbaseClient], val hosts: List[String], val port: String, val base: String, val bucket: String, val user: String, val pass: String, val timeout: Long) extends BucketAPI {

  def connect() = {
    val uris = ArrayBuffer(hosts.map { h => URI.create(s"http://$h:$port/$base") }: _*)
    val cfb = new CouchbaseConnectionFactoryBuilder()
    if (play.api.Play.current.configuration.getBoolean("couchbase.driver.useec").getOrElse(true)) {
      cfb.setListenerExecutorService(ExecutionContextExecutorServiceBridge.apply(Couchbase.couchbaseExecutor(play.api.Play.current)))
    }
    val cf = cfb.buildCouchbaseConnection(uris, bucket, user, pass);
    val client = new CouchbaseClient(cf);
    new CouchbaseBucket(Some(client), hosts, port, base, bucket, user, pass, timeout)
  }

  def disconnect() = {
    client.map(_.shutdown(timeout, TimeUnit.SECONDS))
    new CouchbaseBucket(None, hosts, port, base, bucket, user, pass, timeout)
  }

  def couchbaseClient: CouchbaseClient = {
    client.getOrElse(throw new PlayException(s"Error with bucket ${bucket}", s"Bucket '${bucket}' is not defined or client is not connected"))
  }
}

object Couchbase extends Read with Write with Delete with Counters with Queries with JavaApi with Atomic {

  private val initMessage = "The CouchbasePlugin has not been initialized! Please edit your conf/play.plugins file and add the following line: '400:package org.reactivecouchbase.CouchbasePlugin' (400 is an arbitrary priority and may be changed to match your needs)."
  private val connectMessage = "The CouchbasePlugin doesn't seems to be connected to a Couchbase server. Maybe an error occured!"
  private val couchbaseActorSystem = ActorSystem("couchbase-plugin-system")

  def defaultBucket(implicit app: Application): CouchbaseBucket = app.plugin[CouchbasePlugin] match {
    case Some(plugin) => plugin.buckets.headOption.getOrElse(throw new PlayException("CouchbasePlugin Error", connectMessage))._2
    case _ => throw new PlayException("CouchbasePlugin Error", initMessage)
  }

  def bucket(bucket: String)(implicit app: Application): CouchbaseBucket = buckets(app).get(bucket).getOrElse(throw new PlayException(s"Error with bucket $bucket", s"Bucket '$bucket' is not defined"))
  def cappedBucket(bucket: String, max: Int, reaper: Boolean = true)(implicit app: Application): CappedBucket = buckets(app).get(bucket).map(_ => CappedBucket(bucket, max, reaper)(app)).getOrElse(throw new PlayException(s"Error with bucket $bucket", s"Bucket '$bucket' is not defined"))
  def client(bucket: String)(implicit app: Application): CouchbaseClient = buckets(app).get(bucket).flatMap(_.client).getOrElse(throw new PlayException(s"Error with bucket $bucket", s"Bucket '$bucket' is not defined or client is not connected"))

  def buckets(implicit app: Application): Map[String, CouchbaseBucket] = app.plugin[CouchbasePlugin] match {
    case Some(plugin) => plugin.buckets
    case _ => throw new PlayException("CouchbasePlugin Error", initMessage)
  }

  def couchbaseExecutor(implicit app: Application): ExecutionContext = {
    app.configuration.getObject("couchbase.execution-context.execution-context") match {
      case Some(_) => couchbaseActorSystem.dispatchers.lookup("couchbase.execution-context.execution-context")
      case _ => {
        if (Constants.usePlayEC)
          play.api.libs.concurrent.Execution.Implicits.defaultContext
        else
          throw new PlayException("Configuration issue", "You have to define a 'couchbase.execution-context.execution-context' object in the application.conf file.")
      }
    }
  }

  def apply(
    hosts: List[String] = List(Play.configuration.getString("couchbase.bucket.host").getOrElse("127.0.0.1")),
    port: String = Play.configuration.getString("couchbase.bucket.port").getOrElse("8091"),
    base: String = Play.configuration.getString("couchbase.bucket.base").getOrElse("pools"),
    bucket: String = Play.configuration.getString("couchbase.bucket.bucket").getOrElse("default"),
    user: String = Play.configuration.getString("couchbase.bucket.user").getOrElse(""),
    pass: String = Play.configuration.getString("couchbase.bucket.pass").getOrElse(""),
    timeout: Long = Play.configuration.getLong("couchbase.bucket.timeout").getOrElse(0)): CouchbaseBucket = {
    new CouchbaseBucket(None, hosts, port, base, bucket, user, pass, timeout)
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
