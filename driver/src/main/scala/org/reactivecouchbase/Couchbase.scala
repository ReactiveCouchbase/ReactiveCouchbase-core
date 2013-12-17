package org.reactivecouchbase

import com.couchbase.client.{ CouchbaseConnectionFactoryBuilder, CouchbaseClient }
import java.net.URI
import java.util.concurrent.{ConcurrentHashMap, AbstractExecutorService, TimeUnit}
import collection.JavaConversions._
import collection.mutable.ArrayBuffer
import scala.Some
import scala.concurrent.{ ExecutionContextExecutorService, ExecutionContext }
import akka.actor.ActorSystem
import java.util.Collections
import org.reactivecouchbase.client._
import com.typesafe.config.{Config, ConfigFactory}
import net.spy.memcached.{ReplicateTo, PersistTo}

class CouchbaseBucket( val cbDriver: CouchbaseDriver,
                       val client: Option[CouchbaseClient],
                       val hosts: List[String],
                       val port: String,
                       val base: String,
                       val bucket: String,
                       val user: String,
                       val pass: String,
                       val timeout: Long) extends BucketAPI {

  def connect() = {
    val uris = ArrayBuffer(hosts.map { h => URI.create(s"http://$h:$port/$base") }: _*)
    val cfb = new CouchbaseConnectionFactoryBuilder()
    if (cbDriver.configuration.getBoolean("couchbase.driver.useec").getOrElse(true)) {
      cfb.setListenerExecutorService(ExecutionContextExecutorServiceBridge.apply(cbDriver.executor()))
    }
    val cf = cfb.buildCouchbaseConnection(uris, bucket, user, pass);
    val client = new CouchbaseClient(cf);
    new CouchbaseBucket(cbDriver, Some(client), hosts, port, base, bucket, user, pass, timeout)
  }

  def disconnect() = {
    client.map(_.shutdown(timeout, TimeUnit.SECONDS))
    new CouchbaseBucket(cbDriver, None, hosts, port, base, bucket, user, pass, timeout)
  }

  def couchbaseClient: CouchbaseClient = {
    client.getOrElse(throw new ReactiveCouchbaseException(s"Error with bucket ${bucket}", s"Bucket '${bucket}' is not defined or client is not connected"))
  }

  def driver = cbDriver

  private[reactivecouchbase] val checkFutures = cbDriver.configuration.getBoolean("couchbase.driver.checkfuture").getOrElse(false)
  private[reactivecouchbase] val jsonStrictValidation = cbDriver.configuration.getBoolean("couchbase.json.validate").getOrElse(true)
  private[reactivecouchbase] val failWithOpStatus = cbDriver.configuration.getBoolean("couchbase.failfutures").getOrElse(false)
  private[reactivecouchbase] val ecTimeout: Long = cbDriver.configuration.getLong("couchbase.execution-context.timeout").getOrElse(1000L)
  if (jsonStrictValidation) {
    Logger.info("Failing on bad JSON structure enabled.")
  }
  if (failWithOpStatus) {
    Logger.info("Failing Futures on failed OperationStatus enabled.")
  }
}

class CouchbaseDriver(as: ActorSystem, config: Configuration, logger: LoggerLike) {

  val buckets = new ConcurrentHashMap[String, CouchbaseBucket]

  val bucketsConfig = config.getObjectList("couchbase.buckets")
    .getOrElse(throw new RuntimeException("Can't find any bucket in conf !!!"))
    .map(_.toConfig)
    .map(b => (b.getString("bucket"), b))
    .toMap

  def system() = as
  def executor() = as.dispatcher
  def scheduler() = as.scheduler
  def configuration = config

  def bucket(hosts: List[String], port: String, base: String, bucket: String, user: String, pass: String, timeout: Long) = {
    if (!buckets.containsKey(bucket)) {
      val cb = new CouchbaseBucket(this, None, hosts, port, base, bucket, user, pass, timeout).connect()
      buckets.putIfAbsent(bucket, cb)
    }
    buckets.get(bucket)
  }

  def bucket(name: String): CouchbaseBucket = {
    if (!buckets.containsKey(name)) {
      val cfg = new Configuration(bucketsConfig.get(name).getOrElse(throw new RuntimeException(s"Cannot find bucket $name")))
      val hosts: List[String] = cfg.getString("host").map(_.replace(" ", "")).map(_.split(",").toList).getOrElse(List("127.0.0.1"))
      val port: String =        cfg.getString("port").getOrElse("8091")
      val base: String =        cfg.getString("base").getOrElse("pools")
      val bucket: String =      cfg.getString("bucket").getOrElse("default")
      val user: String =        cfg.getString("user").getOrElse("")
      val pass: String =        cfg.getString("pass").getOrElse("")
      val timeout: Long =         cfg.getLong("timeout").getOrElse(0)
      val cb = new CouchbaseBucket(this, None, hosts, port, base, bucket, user, pass, timeout).connect()
      buckets.putIfAbsent(name, cb)
    }
    buckets.get(name)
  }

  def cappedBucket(name: String, max: Int, reaper: Boolean): CappedBucket = CappedBucket(bucket(name), bucket(name).driver.executor(), max, reaper)
  def cappedBucket(name: String, ec: ExecutionContext, max: Int, reaper: Boolean): CappedBucket = CappedBucket(bucket(name), ec, max, reaper)

  def shutdown() = {
    buckets.foreach(t => t._2.disconnect())
  }
}

object CouchbaseDriver {

  private def defaultSystem = {
    import com.typesafe.config.ConfigFactory
    val config = ConfigFactory.load()
    ActorSystem("ReactiveCouchbaseSystem", config.getConfig("couchbase.actorctx"))
  }

  def apply() = new CouchbaseDriver(defaultSystem, new Configuration(ConfigFactory.load()), Logger)
  def apply(system: ActorSystem) = new CouchbaseDriver(system, new Configuration(ConfigFactory.load()), Logger)
  def apply(system: ActorSystem, config: Configuration) = new CouchbaseDriver(system, config, Logger)
}

object Couchbase extends Read with Write with Delete with Counters with Queries with JavaApi with Atomic {}

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
