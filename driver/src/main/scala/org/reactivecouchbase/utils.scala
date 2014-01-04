package org.reactivecouchbase

import org.slf4j.LoggerFactory
import scala.concurrent.{Promise, Future, ExecutionContext}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import akka.actor.Scheduler
import com.typesafe.config._
import scala.util.control.NonFatal
import scala.concurrent.{ ExecutionContextExecutorService, ExecutionContext }
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, AbstractExecutorService, TimeUnit}

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

class RCLogger(val logger: org.slf4j.Logger) extends LoggerLike  {
  def logger(name: String): LoggerLike = new RCLogger(LoggerFactory.getLogger(name))
  def logger[T](clazz: Class[T]): LoggerLike = new RCLogger(LoggerFactory.getLogger(clazz))
}

object StandaloneLogger extends LoggerLike {
  val logger = LoggerFactory.getLogger("ReactiveCouchbase")
  def logger(name: String): LoggerLike = new RCLogger(LoggerFactory.getLogger(name))
  def logger[T](clazz: Class[T]): LoggerLike = new RCLogger(LoggerFactory.getLogger(clazz))
}

class Configuration(underlying: Config) {

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

object Timeout {
  def timeout[A](message: => A, duration: scala.concurrent.duration.Duration, scheduler: Scheduler)(implicit ec: ExecutionContext): Future[A] = {
    timeout(message, duration.toMillis, TimeUnit.MILLISECONDS, scheduler)
  }
  def timeout[A](message: => A, duration: Long, unit: TimeUnit = TimeUnit.MILLISECONDS, scheduler: Scheduler)(implicit ec: ExecutionContext): Future[A] = {
    val p = Promise[A]()
    scheduler.scheduleOnce(FiniteDuration(duration, unit)) {
      p.success(message)
    }
    p.future
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