package org.reactivecouchbase

import com.couchbase.client.{ CouchbaseConnectionFactoryBuilder, CouchbaseClient }
import java.net.URI
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import net.spy.memcached.metrics.{AbstractMetricCollector, DefaultMetricCollector, MetricType}

import collection.JavaConversions._
import collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import akka.actor.{Scheduler, ActorSystem}
import org.reactivecouchbase.client._
import com.typesafe.config.{Config, ConfigFactory}
import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig}
import org.reactivecouchbase.experimental.CappedBucket

/**
 *
 * Representation of a Couchbase Bucket
 *
 * @param cbDriver ReactiveCouchbase driver associated with this bucket
 * @param client actual Java driver
 * @param hosts list of hosts
 * @param port port of the server
 * @param base base URL
 * @param bucket name of the bucket
 * @param user user for the bucket
 * @param pass password for the bucket
 * @param timeout connection timeout
 */
class CouchbaseBucket( private[reactivecouchbase] val cbDriver: ReactiveCouchbaseDriver,
                       private[reactivecouchbase] val client: Option[CouchbaseClient],
                       val hosts: List[String],
                       val port: String,
                       val base: String,
                       bucket: String,
                       val alias: String,
                       val user: String,
                       val pass: String,
                       val timeout: Long) extends BucketAPI {

  /**
   *
   * Connect the current Java Driver to the Couchbase server
   *
   * @return the connected CouchbaseBucket
   */
  def connect(): CouchbaseBucket = {
    val uris = ArrayBuffer(hosts.map { h => URI.create(s"http://$h:$port/$base") }: _*)
    val cfb = new CouchbaseConnectionFactoryBuilder()
    if (cbDriver.configuration.getBoolean("couchbase.driver.useec").getOrElse(true)) {
      cfb.setListenerExecutorService(ExecutionContextExecutorServiceBridge.apply(cbDriver.executor()))
    }
    cbDriver.configuration.getLong("couchbase.driver.native.authWaitTime").foreach(cfb.setAuthWaitTime)
    cbDriver.configuration.getLong("couchbase.driver.native.obsPollInterval").foreach(cfb.setObsPollInterval)
    cbDriver.configuration.getLong("couchbase.driver.native.obsTimeout").foreach(cfb.setObsTimeout)
    cbDriver.configuration.getLong("couchbase.driver.native.reconnectThresholdTime").foreach(cfb.setReconnectThresholdTime(_, TimeUnit.MILLISECONDS))
    cbDriver.configuration.getInt("couchbase.driver.native.viewConnsPerNode").foreach(cfb.setViewConnsPerNode)
    cbDriver.configuration.getInt("couchbase.driver.native.viewTimeout").foreach(cfb.setViewTimeout)
    cbDriver.configuration.getInt("couchbase.driver.native.viewWorkerSize").foreach(cfb.setViewWorkerSize)
    cbDriver.configuration.getBoolean("couchbase.driver.native.useNagleAlgorithm").foreach(cfb.setUseNagleAlgorithm)
    cbDriver.configuration.getBoolean("couchbase.driver.native.daemon").foreach(cfb.setDaemon)
    cbDriver.configuration.getInt("couchbase.driver.native.timeoutExceptionThreshold").foreach(cfb.setTimeoutExceptionThreshold)
    cbDriver.configuration.getBoolean("couchbase.driver.native.shouldOptimize").foreach(cfb.setShouldOptimize)
    cbDriver.configuration.getLong("couchbase.driver.native.maxReconnectDelay").foreach(cfb.setMaxReconnectDelay)
    cbDriver.configuration.getLong("couchbase.driver.native.opQueueMaxBlockTime").foreach(cfb.setOpQueueMaxBlockTime)
    cbDriver.configuration.getLong("couchbase.driver.native.opTimeout").foreach(cfb.setOpTimeout)
    cbDriver.configuration.getLong("couchbase.driver.native.maxReconnectDelay").foreach(cfb.setMaxReconnectDelay)
    cbDriver.configuration.getInt("couchbase.driver.native.readBufferSize").foreach(cfb.setReadBufferSize)

    // Sets the metric Collector, ths class must extent AbstractMetricCollector
    cbDriver.configuration.getString("couchbase.driver.native.setMetricCollector").foreach( x => {
      cfb.setMetricCollector(Class.forName(x).newInstance().asInstanceOf[AbstractMetricCollector]);
    })

    // SInce MetricType is an enum it iteratetes through possible values and sets the correct MetricType
    MetricType.values().find(prop => prop.toString.equalsIgnoreCase(cbDriver.configuration.getString("couchbase.driver.native.enableMetrics").getOrElse(MetricType.OFF.name()))).foreach(cfb.setEnableMetrics)


    val cf = cfb.buildCouchbaseConnection(uris, bucket, user,pass)
    var client :CouchbaseClient = new CouchbaseClient(cf)
    if (jsonStrictValidation) {
      driver.logger.info("Failing on bad JSON structure enabled.")
    }
    if (failWithOpStatus) {
      driver.logger.info("Failing Futures on failed OperationStatus enabled.")
    }
    new CouchbaseBucket(cbDriver, Some(client), hosts, port, base, bucket, alias, user, pass, timeout)
  }

  def logger = cbDriver.logger

  /**
   *
   * Disconnect the current Java Driver from the Couchbase server
   *
   * @return the disconnected CouchbaseBucket
   */
  def disconnect(): CouchbaseBucket = {
    client.map(_.shutdown(timeout, TimeUnit.SECONDS))
    CappedBucket.clear(alias)
    new CouchbaseBucket(cbDriver, None, hosts, port, base, bucket, alias, user, pass, timeout)
  }

  /**
   * @return the actual Java Driver
   */
  def couchbaseClient: CouchbaseClient = {
    client.getOrElse(throw new ReactiveCouchbaseException(s"Error with bucket ${bucket}", s"Bucket '${bucket}' is not defined or client is not connected"))
  }

  /**
   * @return the actual ReactiveCouchbaseDriver
   */
  def driver: ReactiveCouchbaseDriver = cbDriver

  private[reactivecouchbase] val blockInFutures = cbDriver.configuration.getBoolean("couchbase.driver.blockinfutures").getOrElse(false)
  private[reactivecouchbase] val block = cbDriver.configuration.getBoolean("couchbase.driver.block").getOrElse(false)
  private[reactivecouchbase] val enableOperationTimeout = cbDriver.configuration.getBoolean("couchbase.driver.enableoperationtimeout").getOrElse(false)
  private[reactivecouchbase] val doubleCheck = cbDriver.configuration.getBoolean("couchbase.driver.doublecheck").getOrElse(false)

  /**
   * Check if Futures from Java Driver are failed. If so, fails scala Future
   */
  private[reactivecouchbase] val checkFutures = cbDriver.configuration.getBoolean("couchbase.driver.checkfuture").getOrElse(false)

  /**
   * Fails Json processing if Json structure is wrong
   */
  private[reactivecouchbase] val jsonStrictValidation = cbDriver.configuration.getBoolean("couchbase.json.validate").getOrElse(true)

  /**
   * Fail Scala futures if OperationStatus is failed
   */
  private[reactivecouchbase] val failWithOpStatus = cbDriver.configuration.getBoolean("couchbase.failfutures").getOrElse(false)

  private[reactivecouchbase] val failWithNonStringDoc = cbDriver.configuration.getBoolean("couchbase.failonnonstring").getOrElse(false)

  /**
   * Timeout
   */
  private[reactivecouchbase] val ecTimeout: Long = cbDriver.configuration.getLong("couchbase.akka.timeout").getOrElse(60000L)

  /**
   * Use experimental Query API instead of Java Drivers one
   */
  private[reactivecouchbase] val useExperimentalQueries: Boolean = cbDriver.configuration.getBoolean("couchbase.experimental.queries").getOrElse(false)
  private[reactivecouchbase] val workersNbr: Int = cbDriver.configuration.getInt("couchbase.experimental.workers").getOrElse(10)

  /**
   * Optional N1QL host
   */
  private[reactivecouchbase] val N1QLHost = driver.configuration.getString("couchbase.n1ql.host")

  /**
   * Optional N1QL port
   */
  private[reactivecouchbase] val N1QLPort = driver.configuration.getString("couchbase.n1ql.port")

  /**
   * Configuration for HTTP client
   */
  private[reactivecouchbase] val config: AsyncHttpClientConfig = new AsyncHttpClientConfig.Builder()
    .setAllowPoolingConnection(cbDriver.configuration.getBoolean("couchbase.http.pool").getOrElse(true))
    .setCompressionEnabled(cbDriver.configuration.getBoolean("couchbase.http.compression").getOrElse(true))
    .setRequestTimeoutInMs(cbDriver.configuration.getInt("couchbase.http.reqtimeout").getOrElse(60000))
    .setIdleConnectionInPoolTimeoutInMs(cbDriver.configuration.getInt("couchbase.http.idlepool").getOrElse(60000))
    .setIdleConnectionTimeoutInMs(cbDriver.configuration.getInt("couchbase.http.idleconnection").getOrElse(60000))
    .setMaximumConnectionsTotal(cbDriver.configuration.getInt("couchbase.http.maxTotalConnections").getOrElse(-1))
    .setMaximumConnectionsPerHost(cbDriver.configuration.getInt("couchbase.http.maxConnectionsPerHost").getOrElse(-1))
    .build()

  /**
   * The HTTP client dedicated for this bucket (used for view queries and N1QL support)
   */
  private[reactivecouchbase] val httpClient: AsyncHttpClient = new AsyncHttpClient(config)
}

/**
 * Driver to access a Couchbase server
 *
 *
 * Example :
 * {{{
 *   val driver = ReactiveCouchbaseDriver()
 *   val bucket = driver.bucket("default")
 *   val capped = driver.cappedBucket("default", 100, true)
 * }}}
 */
class ReactiveCouchbaseDriver(as: ActorSystem, config: Configuration, log: LoggerLike, val mode: Mode) {

  /**
   * All the buckets managed by this driver
   */
  val buckets = new ConcurrentHashMap[String, CouchbaseBucket]

  /**
   * Config for each bucket
   */
  val bucketsConfig: Map[String, Config] = config.getObjectList("couchbase.buckets")
    .getOrElse(throw new ReactiveCouchbaseException("No configuration", "Can't find any bucket in conf !!!"))
    .map(c => new Configuration(c.toConfig))
    .map(b => (b.getString("alias").getOrElse(b.getString("bucket").getOrElse(throw new ReactiveCouchbaseException("Error", "No bucket name :("))), b.underlying))
    .toMap

  /**
   * @return actor system for this driver
   */
  def system(): ActorSystem = as

  /**
   * @return ExecutionContext for this driver
   */
  def executor(): ExecutionContext = as.dispatcher

  /**
   * @return Akka Scheduler for this driver
   */
  def scheduler(): Scheduler = as.scheduler

  /**
   * @return Configuration for this driver
   */
  def configuration: Configuration = config

  /**
   * @return Logger wrapper for this driver
   */
  def logger: LoggerLike = log

  def bucket(hosts: List[String], port: String, base: String, bucket: String, alias: String, user: String, pass: String, timeout: Long): CouchbaseBucket = {
    if (!buckets.containsKey(alias)) {
      buckets.putIfAbsent(alias, new CouchbaseBucket(this, None, hosts, port, base, bucket, alias, user, pass, timeout).connect())
    }
    buckets.get(alias)
  }

  def bucket(name: String): CouchbaseBucket = {
    if (!buckets.containsKey(name)) {
      val cfg = new Configuration(bucketsConfig.get(name).getOrElse(throw new ReactiveCouchbaseException("No bucket", s"Cannot find bucket $name")))
      val hosts: List[String] = cfg.underlying.getValue("host").unwrapped() match {
        case s: String if s.trim.isEmpty => List[String]("127.0.0.1")
        case s: String if !s.trim.isEmpty => List(s)
        case a: java.util.ArrayList[String] if !a.isEmpty => a.toList
        case a: java.util.ArrayList[String] if a.isEmpty => List[String]("127.0.0.1")
        case null => List[String]("127.0.0.1")
      }
      val port: String =        cfg.getString("port").getOrElse("8091")
      val base: String =        cfg.getString("base").getOrElse("pools")
      val bucket: String =      cfg.getString("bucket").getOrElse("default")
      val alias: String =       cfg.getString("alias").getOrElse(bucket)
      val user: String =        cfg.getString("user").getOrElse("")
      val pass: String =        cfg.getString("pass").getOrElse("")
      val timeout: Long =       cfg.getLong("timeout").getOrElse(0)
      val cb = new CouchbaseBucket(this, None, hosts, port, base, bucket, alias, user, pass, timeout).connect()
      buckets.putIfAbsent(name, cb)
    }
    buckets.get(name)
  }

  /**
   *
   * Provide a Bucket as Capped bucket.
   * A capped bucket is a special bucket where insertion order of documents is kept
   *
   * @param name name of the Couchbase bucket
   * @param max max number of element in the capped bucket
   * @param reaper activate reaper to delete elements when the capped bucket is full (ie. bigger than max)
   * @return a capped bucket
   */
  def cappedBucket(name: String, max: Int, reaper: Boolean): CappedBucket = CappedBucket(() => bucket(name), bucket(name).driver.executor(), max, reaper)

  /**
   *
   * Provide a Bucket as Capped bucket.
   * A capped bucket is a special bucket where insertion order of documents is kept
   *
   * @param name name of the Couchbase bucket
   * @param ec the ExecutionContext for async processing
   * @param max max number of element in the capped bucket
   * @param reaper activate reaper to delete elements when the capped bucket is full (ie. bigger than max)
   * @return a capped bucket
   */
  def cappedBucket(name: String, ec: ExecutionContext, max: Int, reaper: Boolean): CappedBucket = CappedBucket(() => bucket(name), ec, max, reaper)

  /**
   *
   * Create a N1QL query
   *
   * @param query the actual query written in N1QL query language
   * @return a new N1QL query
   */
  def N1QL(query: String): N1QLQuery = {
    CouchbaseN1QL.N1QL(query)(this.buckets.head._2)
  }

  /**
   * Shutdown this driver
   */
  def shutdown() {
    buckets.foreach(t => {
      t._2.disconnect()
      t._2.httpClient.close()
    })
    CappedBucket.clearCache()
    system().shutdown()
  }
}

/**
 * Companion object to build ReactiveCouchbaseDriver
 */
object ReactiveCouchbaseDriver {

  /**
   *
   * Create the default ActorSystem if not provided
   *
   * @return the default actor system
   */
  private def defaultSystem = {
    import com.typesafe.config.ConfigFactory
    val config = ConfigFactory.load()
    ActorSystem("ReactiveCouchbaseSystem", config.getConfig("couchbase.akka"))
  }

  /**
   * @return a new ReactiveCouchbaseDriver with default actor system and configuration
   */
  def apply(): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(defaultSystem, new Configuration(ConfigFactory.load()), StandaloneLogger.configure(), Prod())

  /**
   * @param system a custom ActorSystem provided by the user
   * @return a new ReactiveCouchbaseDriver with default configuration
   */
  def apply(system: ActorSystem): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(system, new Configuration(ConfigFactory.load()), StandaloneLogger.configure(), Prod())

  /**
   * @param system a custom ActorSystem provided by the user
   * @param config a custom configuration provided by the user
   * @return a new ReactiveCouchbaseDriver
   */
  def apply(system: ActorSystem, config: Configuration): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(system, config, StandaloneLogger.configure(), Prod())

  /**
   * @param system a custom ActorSystem provided by the user
   * @param config a custom configuration provided by the user
   * @param logger a custom logger wrapper provided by the user
   * @return a new ReactiveCouchbaseDriver
   */
  def apply(system: ActorSystem, config: Configuration, logger: LoggerLike): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(system, config, logger, Prod())

  /**
   * @param system a custom ActorSystem provided by the user
   * @param config a custom configuration provided by the user
   * @param logger a custom logger wrapper provided by the user
   * @param mode mode of the driver
   * @return a new ReactiveCouchbaseDriver
   */
  def apply(
     system: ActorSystem = defaultSystem,
     config: Configuration = new Configuration(ConfigFactory.load()),
     logger: LoggerLike = StandaloneLogger.configure(),
     mode: Mode = Prod()
  ): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(system, config, logger, mode)
}

/**
 * Main API for Couchbase generic operations.
 */
object Couchbase extends Read with Write with Delete with Counters with Queries with JavaApi with Atomic {}

trait Mode {
  def name: String
}

case class Dev() extends Mode {
  def name: String = "dev_"
}

case class Prod() extends Mode {
  def name: String = ""
}

case class Test() extends Mode {
  def name: String = ""
}