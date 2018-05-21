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


  val EVENT_TAG_RICH = 0L
  val EVENT_TAG_COMPACT = 1L

}
