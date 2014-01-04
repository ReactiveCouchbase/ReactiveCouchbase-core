package org.reactivecouchbase

import com.couchbase.client.{ CouchbaseConnectionFactoryBuilder, CouchbaseClient }
import java.net.URI
import java.util.concurrent.{ConcurrentHashMap, AbstractExecutorService, TimeUnit}
import collection.JavaConversions._
import collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContextExecutorService, ExecutionContext }
import akka.actor.{Scheduler, ActorSystem}
import java.util.Collections
import org.reactivecouchbase.client._
import com.typesafe.config.{Config, ConfigFactory}

class CouchbaseBucket( private[reactivecouchbase] val cbDriver: ReactiveCouchbaseDriver,
                       private[reactivecouchbase] val client: Option[CouchbaseClient],
                       val hosts: List[String],
                       val port: String,
                       val base: String,
                       val bucket: String,
                       val user: String,
                       val pass: String,
                       val timeout: Long) extends BucketAPI {

  def connect(): CouchbaseBucket = {
    val uris = ArrayBuffer(hosts.map { h => URI.create(s"http://$h:$port/$base") }: _*)
    val cfb = new CouchbaseConnectionFactoryBuilder()
    if (cbDriver.configuration.getBoolean("couchbase.driver.useec").getOrElse(true)) {
      cfb.setListenerExecutorService(ExecutionContextExecutorServiceBridge.apply(cbDriver.executor()))
    }
    val cf = cfb.buildCouchbaseConnection(uris, bucket, user, pass)
    val client = new CouchbaseClient(cf)
    if (jsonStrictValidation) {
      driver.logger.info("Failing on bad JSON structure enabled.")
    }
    if (failWithOpStatus) {
      driver.logger.info("Failing Futures on failed OperationStatus enabled.")
    }
    new CouchbaseBucket(cbDriver, Some(client), hosts, port, base, bucket, user, pass, timeout)
  }

  def disconnect(): CouchbaseBucket = {
    client.map(_.shutdown(timeout, TimeUnit.SECONDS))
    new CouchbaseBucket(cbDriver, None, hosts, port, base, bucket, user, pass, timeout)
  }

  def couchbaseClient: CouchbaseClient = {
    client.getOrElse(throw new ReactiveCouchbaseException(s"Error with bucket ${bucket}", s"Bucket '${bucket}' is not defined or client is not connected"))
  }

  def driver: ReactiveCouchbaseDriver = cbDriver

  private[reactivecouchbase] val checkFutures = cbDriver.configuration.getBoolean("couchbase.driver.checkfuture").getOrElse(false)
  private[reactivecouchbase] val jsonStrictValidation = cbDriver.configuration.getBoolean("couchbase.json.validate").getOrElse(true)
  private[reactivecouchbase] val failWithOpStatus = cbDriver.configuration.getBoolean("couchbase.failfutures").getOrElse(false)
  private[reactivecouchbase] val ecTimeout: Long = cbDriver.configuration.getLong("couchbase.actorctx.timeout").getOrElse(1000L)
}

class ReactiveCouchbaseDriver(as: ActorSystem, config: Configuration, log: LoggerLike) {

  val buckets = new ConcurrentHashMap[String, CouchbaseBucket]

  val bucketsConfig: Map[String, Config] = config.getObjectList("couchbase.buckets")
    .getOrElse(throw new RuntimeException("Can't find any bucket in conf !!!"))
    .map(_.toConfig)
    .map(b => (b.getString("bucket"), b))
    .toMap

  def system(): ActorSystem = as
  def executor(): ExecutionContext = as.dispatcher
  def scheduler(): Scheduler = as.scheduler
  def configuration: Configuration = config
  def logger: LoggerLike = log

  def bucket(hosts: List[String], port: String, base: String, bucket: String, user: String, pass: String, timeout: Long): CouchbaseBucket = {
    if (!buckets.containsKey(bucket)) {
      buckets.putIfAbsent(bucket, new CouchbaseBucket(this, None, hosts, port, base, bucket, user, pass, timeout).connect())
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

  def N1QL(query: String): N1QLQuery = {
    CouchbaseN1QL.N1QL(query)(this)
  }

  def shutdown() {
    buckets.foreach(t => t._2.disconnect())
  }
}

object ReactiveCouchbaseDriver {

  private def defaultSystem = {
    import com.typesafe.config.ConfigFactory
    val config = ConfigFactory.load()
    ActorSystem("ReactiveCouchbaseSystem", config.getConfig("couchbase.actorctx"))
  }

  def apply(): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(defaultSystem, new Configuration(ConfigFactory.load()), StandaloneLogger)
  def apply(system: ActorSystem): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(system, new Configuration(ConfigFactory.load()), StandaloneLogger)
  def apply(system: ActorSystem, config: Configuration): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(system, config, StandaloneLogger)
  def apply(system: ActorSystem, config: Configuration, logger: LoggerLike): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(system, config, logger)
}

object Couchbase extends Read with Write with Delete with Counters with Queries with JavaApi with Atomic {}
