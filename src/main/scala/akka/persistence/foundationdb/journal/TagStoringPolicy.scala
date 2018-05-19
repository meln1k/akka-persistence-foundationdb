package akka.persistence.foundationdb.journal

import akka.persistence.query.scaladsl.{CurrentEventsByTagQuery, EventsByTagQuery}
import com.typesafe.config.Config
import scala.collection.JavaConverters._

sealed trait TagStoringPolicy


object TagStoringPolicy {

  /**
    * All tags indexes will be stored using a rich representation and will contain whole event data,
    * e.g. (tag, versionstamp) -> persistentRepr.
    *
    * Event log will consume double amount of space, however reads with [[EventsByTagQuery#eventsByTag]] and
    * [[CurrentEventsByTagQuery#currentEventsByTag]] will have maximum performance.
    */
  case object AlwaysRich extends TagStoringPolicy

  /**
    * All tags indexes will be stored using a rich representation by default, however it's possible to use
    * compact representation for specific tags.
    *
    * @param compactTags tags which will be stored using the compact representation
    */
  case class DefaultRich(compactTags: Set[String]) extends TagStoringPolicy

  /**
    * All tags indexes will be stored using a compact representation by default, however it's possible to use
    * rich representation for specific tags.
    *
    * @param richTags tags which will be stored using the rich representation
    */
  case class DefaultCompact(richTags: Set[String]) extends TagStoringPolicy

  /**
    * All tags indexes will be stored using a compact representation and will contain only a pointer to the event payload,
    * e.g. (tag, versionstamp) -> (persistenceId, seqNo).
    *
    * Event log will consume the smallest amount of space, however reads with [[EventsByTagQuery#eventsByTag]] and
    * [[CurrentEventsByTagQuery#currentEventsByTag]] will be slower compared to the rich representation.
    */
  case object AlwaysCompact extends TagStoringPolicy

  def fromConfig(cfg: Config): TagStoringPolicy = {
    cfg.getString("tag-storing-policy") match {
      case "AlwaysRich" =>
        AlwaysRich
      case "DefaultRich" =>
        val compactTags = cfg.getStringList("compact-tags").asScala.toSet
        if (compactTags.isEmpty) {
          AlwaysRich
        } else {
          DefaultRich(compactTags)
        }
      case "DefaultCompact" =>
        val richTags = cfg.getStringList("rich-tags").asScala.toSet
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

}
