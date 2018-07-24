package akka.persistence.foundationdb.query

import akka.persistence.query.Offset
import com.apple.foundationdb.tuple.Versionstamp

/**
  * Corresponds to an ordered unique versionstamp of the events. Note that the corresponding
  * offset of each event is provided in the [[akka.persistence.query.EventEnvelope]],
  * which makes it possible to resume the stream at a later point from a given offset.
  *
  * The `offset` is exclusive, i.e. the event with the exact same sequence number will not be included
  * in the returned stream. This means that you can use the offset that is returned in `EventEnvelope`
  * as the `offset` parameter in a subsequent query.
  */
final case class VersionstampOffset(value: Versionstamp) extends Offset with Ordered[VersionstampOffset] {
  override def compare(that: VersionstampOffset): Int = value.compareTo(that.value)
}
