package akka.persistence.foundationdb

sealed trait TagType
final case class CompactTag(persistenceId: String, sequenceNr: Long) extends TagType
final case class RichTag(payload: Array[Byte]) extends TagType
