package akka.persistence.foundationdb.journal

import akka.actor.ActorSystem
import akka.persistence.foundationdb.FoundationDbPluginConfig
import akka.persistence.foundationdb.journal.TagStoringPolicy._
import com.typesafe.config.Config

import scala.collection.JavaConverters._

class FoundationDbJournalConfig(system: ActorSystem, config: Config) extends FoundationDbPluginConfig(system, config) {

  val tagStoringPolicy: TagStoringPolicy = {
    config.getString("tag-storing-policy") match {
      case "AlwaysRich" =>
        AlwaysRich

      case "DefaultRich" =>
        val compactTags = config.getStringList("compact-tags").asScala.toSet
        if (compactTags.isEmpty) {
          AlwaysRich
        } else {
          DefaultRich(compactTags)
        }

      case "DefaultCompact" =>
        val richTags = config.getStringList("rich-tags").asScala.toSet
        if (richTags.isEmpty) {
          AlwaysCompact
        } else {
          DefaultCompact(richTags)
        }

      case "AlwaysCompact" =>
        AlwaysCompact

      case unknown =>
        throw new IllegalArgumentException(s"Unknown tag storing policy: $unknown")
    }
  }

  val checkJournalCorruption = config.getBoolean("check-journal-corruption")
}
