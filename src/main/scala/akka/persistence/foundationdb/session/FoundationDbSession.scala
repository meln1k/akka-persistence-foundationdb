/*
 * Copyright (C) 2018 Nikita Melkozerov. <n.melkozerov at gmail dot com>
 */

package akka.persistence.foundationdb.session

import akka.actor.{ActorSystem, NoSerializationVerificationNeeded}
import akka.event.LoggingAdapter
import akka.persistence.foundationdb.{Directories, FoundationDbPluginConfig, SessionProvider}
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.{Database, ReadTransaction, Transaction}

import scala.compat.java8.FutureConverters._
import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.collection.JavaConverters._

/**
  * Data Access Object for FoundationDB.
  *
  * All methods are non-blocking.
  */
final class FoundationDbSession(
    system: ActorSystem,
    sessionProvider: SessionProvider,
    log: LoggingAdapter,
    config: FoundationDbPluginConfig
) extends NoSerializationVerificationNeeded {

  private implicit val ec =
    system.dispatchers.lookup("foundationdb-plugin-default-dispatcher")

  private val _underlyingSession: Future[Database] = {
    val session = sessionProvider.connect()
    session.failed.foreach { e =>
      log.warning(
        "Failed to connect to FoundationDb. Caused by: {}",
        e.getMessage
      )
    }
    session
  }

  //TODO introduce a retry

  def underlying(): Future[Database] = _underlyingSession

  def close(): Unit = _underlyingSession.foreach(_.close())

  def runAsync[T](transaction: Transaction => Future[T]): Future[T] =
    underlying().flatMap { implicit db =>
      db.runAsync(tx => transaction(tx).toJava.toCompletableFuture).toScala
    }

  def readAsync[T](transaction: ReadTransaction => Future[T]): Future[T] =
    underlying().flatMap { implicit db =>
      db.readAsync(tx => transaction(tx).toJava.toCompletableFuture).toScala
    }

  def resolveDirectories(): Future[Directories] = async {
    import config._
    val directoryLayer = new DirectoryLayer()
    val db = await(underlying())
    val pluginDirectory = await(directoryLayer.createOrOpen(db, List(pluginDirectoryName).asJava).toScala)
    val messagesDirectory = await(
      pluginDirectory
        .createOrOpen(db, List(messagesDirectoryName).asJava)
        .toScala)
    val sequenceNrDirectory = await(
      pluginDirectory
        .createOrOpen(db, List(sequenceNrDirectoryName).asJava)
        .toScala)
    val tagsDirectory = await(pluginDirectory.createOrOpen(db, List(tagsDirectoryName).asJava).toScala)
    val tagWatchesDirectory = await(
      pluginDirectory
        .createOrOpen(db, List(tagWatchesDirectoryName).asJava)
        .toScala)
    val snapshotDirectory = await(
      pluginDirectory
        .createOrOpen(db, List(snapshotsDirectoryName).asJava)
        .toScala)
    Directories(pluginDirectory,
                messagesDirectory,
                sequenceNrDirectory,
                tagsDirectory,
                tagWatchesDirectory,
                snapshotDirectory)
  }

}
