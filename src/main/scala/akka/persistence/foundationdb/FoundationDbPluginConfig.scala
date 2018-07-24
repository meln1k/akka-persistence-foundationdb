package akka.persistence.foundationdb


import akka.actor.ActorSystem

import com.typesafe.config.Config

import scala.collection.JavaConverters._

class FoundationDbPluginConfig(system: ActorSystem, config: Config) {

  val sessionProvider: SessionProvider = SessionProvider(system, config)

  val pluginDirectoryName: String = config.getString("directory")// directoryLayer.createOrOpen(db, List(config.getString("directory")).asJava).get()
  val tagsDirectoryName: String = "tags"
  val messagesDirectoryName: String = "messages"
  val sequenceNrDirectoryName: String = "seqNr"
  val tagWatchesDirectoryName: String = "tagWatches"
  val snapshotsDirectoryName: String = "snapshots"


//  val TagWatchShards: Map[String, Int] = config
//    .getObject("tag-watch-shards")
//    .asScala
//    .map { case (tagName, shards) =>
//      val nrOfShards = shards.unwrapped().asInstanceOf[java.lang.Integer].toInt
//      require(nrOfShards > 0, "number of shards must be positive")
//      tagName -> nrOfShards
//    }.toMap


}
