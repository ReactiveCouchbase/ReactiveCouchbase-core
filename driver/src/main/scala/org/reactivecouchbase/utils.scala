package org.reactivecouchbase

import org.slf4j.LoggerFactory
import scala.concurrent.{Promise, Future}
import scala.concurrent.duration.FiniteDuration
import akka.actor.Scheduler
import com.typesafe.config._
import scala.util.control.NonFatal
import scala.concurrent.{ ExecutionContextExecutorService, ExecutionContext }
import java.util.Collections
import java.util.concurrent.{Executors, LinkedBlockingDeque, AbstractExecutorService, TimeUnit}
import scala.util.Try

/**
 * Trait to wrap Logger
 */
trait LoggerLike {

  val logger: org.slf4j.Logger

  def isTraceEnabled = logger.isTraceEnabled

  def isDebugEnabled = logger.isDebugEnabled

  def isInfoEnabled = logger.isInfoEnabled

  def isWarnEnabled = logger.isWarnEnabled

  def isErrorEnabled = logger.isErrorEnabled

  def trace(message: => String) {
    if (logger.isTraceEnabled) logger.trace(message)
  }

  def trace(message: => String, error: => Throwable) {
    if (logger.isTraceEnabled) logger.trace(message, error)
  }

  def debug(message: => String) {
    if (logger.isDebugEnabled) logger.debug(message)
  }

  def debug(message: => String, error: => Throwable) {
    if (logger.isDebugEnabled) logger.debug(message, error)
  }

  def info(message: => String) {
    if (logger.isInfoEnabled) logger.info(message)
  }

  def info(message: => String, error: => Throwable) {
    if (logger.isInfoEnabled) logger.info(message, error)
  }

  def warn(message: => String) {
    if (logger.isWarnEnabled) logger.warn(message)
  }

  def warn(message: => String, error: => Throwable) {
    if (logger.isWarnEnabled) logger.warn(message, error)
  }

  def error(message: => String) {
    if (logger.isErrorEnabled) logger.error(message)
  }

  def error(message: => String, error: => Throwable) {
    if (logger.isErrorEnabled) logger.error(message, error)
  }

  def logger(name: String): LoggerLike
  def logger[T](clazz: Class[T]): LoggerLike
}

/**
 *
 * Standard logger
 *
 * @param logger wrapped logger
 */
class RCLogger(val logger: org.slf4j.Logger) extends LoggerLike  {
  def logger(name: String): LoggerLike = new RCLogger(LoggerFactory.getLogger(name))
  def logger[T](clazz: Class[T]): LoggerLike = new RCLogger(LoggerFactory.getLogger(clazz))
}

/**
 *
 * Base logger for ReactiveCouchbase
 *
 */
object StandaloneLogger extends LoggerLike {
  lazy val logger = LoggerFactory.getLogger("ReactiveCouchbase")
  def logger(name: String): LoggerLike = new RCLogger(LoggerFactory.getLogger(name))
  def logger[T](clazz: Class[T]): LoggerLike = new RCLogger(LoggerFactory.getLogger(clazz))

  def configure(): LoggerLike = {
//    {
//      import java.util.logging._
//      Option(java.util.logging.Logger.getLogger("")).map { root =>
//        root.setLevel(java.util.logging.Level.FINEST)
//        root.getHandlers.foreach(root.removeHandler(_))
//      }
//    }
//    {
//      import org.slf4j._
//      import ch.qos.logback.classic.joran._
//      import ch.qos.logback.core.util._
//      import ch.qos.logback.classic._
//      try {
//        val ctx = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
//        val configurator = new JoranConfigurator
//        configurator.setContext(ctx)
//        ctx.reset()
//        try {
//          val configResource =
//            Option(System.getProperty("logger.resource"))
//              .map(s => if (s.startsWith("/")) s.drop(1) else s)
//              .map(r => Option(this.getClass.getClassLoader.getResource(r))
//              .getOrElse(new java.net.URL("file:///" + System.getProperty("logger.resource")))
//            ).orElse {
//              Option(System.getProperty("logger.file")).map(new java.io.File(_).toURI.toURL)
//            }.orElse {
//              Option(System.getProperty("logger.url")).map(new java.net.URL(_))
//            }.orElse {
//              Option(this.getClass.getClassLoader.getResource("application-logger.xml"))
//                .orElse(Option(this.getClass.getClassLoader.getResource("logger.xml")))
//            }
//          configResource.foreach { url => configurator.doConfigure(url) }
//        } catch {
//          case NonFatal(e) => e.printStackTrace()
//        }
//        StatusPrinter.printIfErrorsOccured(ctx)
//      } catch {
//        case NonFatal(_) =>
//      }
//      this
//    }
    this
  }
}

class Configuration(val underlying: Config) {

  private def readValue[T](path: String, v: => T): Option[T] = {
    try {
      Option(v)
    } catch {
      case e: ConfigException.Missing => None
      case NonFatal(e) => throw reportError(path, e.getMessage, Some(e))
    }
  }

  def getString(path: String, validValues: Option[Set[String]] = None): Option[String] = readValue(path, underlying.getString(path)).map { value =>
    validValues match {
      case Some(values) if values.contains(value) => value
      case Some(values) if values.isEmpty => value
      case Some(values) => throw reportError(path, "Incorrect value, one of " + (values.reduceLeft(_ + ", " + _)) + " was expected.")
      case None => value
    }
  }

  def getInt(path: String): Option[Int] = readValue(path, underlying.getInt(path))

  def getBoolean(path: String): Option[Boolean] = readValue(path, underlying.getBoolean(path))

  def getDouble(path: String): Option[Double] = readValue(path, underlying.getDouble(path))

  def getLong(path: String): Option[Long] = readValue(path, underlying.getLong(path))

  def getDoubleList(path: String): Option[java.util.List[java.lang.Double]] = readValue(path, underlying.getDoubleList(path))

  def getIntList(path: String): Option[java.util.List[java.lang.Integer]] = readValue(path, underlying.getIntList(path))

  def getList(path: String): Option[ConfigList] = readValue(path, underlying.getList(path))

  def getLongList(path: String): Option[java.util.List[java.lang.Long]] = readValue(path, underlying.getLongList(path))

  def getObjectList(path: String): Option[java.util.List[_ <: ConfigObject]] = readValue[java.util.List[_ <: ConfigObject]](path, underlying.getObjectList(path))

  def getStringList(path: String): Option[java.util.List[java.lang.String]] = readValue(path, underlying.getStringList(path))

  def getObject(path: String): Option[ConfigObject] = readValue(path, underlying.getObject(path))

  def reportError(path: String, message: String, e: Option[Throwable] = None): RuntimeException = {
    new RuntimeException(message, e.getOrElse(new RuntimeException))
  }
}

/**
 * API to create Future that success after a duration
 */
object Timeout {

  /**
   *
   * Create a Future that success after a duration
   *
   * @param message the message wrapped by the future after success
   * @param duration the duration after which Future is a success
   * @param scheduler the Scheduler to manager duration
   * @param ec the Execution Context for Future execution
   * @tparam A Type of the message
   * @return the Future created
   */
  def timeout[A](message: => A, duration: scala.concurrent.duration.Duration, scheduler: Scheduler)(implicit ec: ExecutionContext): Future[A] = {
    timeout(message, duration.toMillis, TimeUnit.MILLISECONDS, scheduler)
  }

  /**
   *
   * Create a Future that success after a duration
   *
   * @param message the message wrapped by the future after success
   * @param duration the duration after which Future is a success
   * @param unit the unit of duration
   * @param scheduler the Scheduler to manager duration
   * @param ec the Execution Context for Future execution
   * @tparam A Type of the message
   * @return the Future created
   */
  def timeout[A](message: => A, duration: Long, unit: TimeUnit = TimeUnit.MILLISECONDS, scheduler: Scheduler)(implicit ec: ExecutionContext): Future[A] = {
    val p = Promise[A]()
    scheduler.scheduleOnce(FiniteDuration(duration, unit)) {
      p.success(message)
    }
    p.future
  }
}

/**
 * Bridge to transform any ExecutionContext in ExecutorService
 */
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

import akka.actor.Actor

private case class FutureDone()

trait FutureAwareActor extends Actor {

  private val internalEc = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

  private def activeLoop: Receive = {
    case message => {
      context.become(waitingLoop)
      processNext(message)
    }
  }

  private def processNext(message: Any) = {
    message match {
      case msg if receiveAsync.isDefinedAt(msg) => {
        val ref = self
        receiveAsync(msg).onComplete {
          case _ => ref ! FutureDone()
        }(internalEc)
      }
      case _ =>
    }
  }

  private def dequeueAnProcessNext() = {
    Try(waitingMessages.dequeue()).toOption match {
      case None => context.become(activeLoop)
      case Some(message) => processNext(message)
    }
  }

  private val waitingMessages = collection.mutable.Queue[Any]()

  private def waitingLoop: Receive = {
    case FutureDone() => dequeueAnProcessNext()
    case message: AnyRef => waitingMessages.enqueue(message)
  }

  type ReceiveAsync = PartialFunction[Any, Future[_]]

  def receiveAsync: ReceiveAsync

  def receive = activeLoop
}
