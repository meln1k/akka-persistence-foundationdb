package akka.persistence.foundationdb.journal

import akka.persistence.foundationdb.Directories
import akka.util.ByteString
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}

private[foundationdb] sealed trait Key {
  def subspace: Subspace
  def tuple: Tuple
  def bytes: Array[Byte] = subspace.pack(tuple)
}

trait VersionstampedKey extends Key {
  def versionstamp: Versionstamp

  override def bytes: Array[Byte] = {
    if (versionstamp.isComplete) {
      subspace.pack(tuple)
    } else {
      subspace.packWithVersionstamp(tuple)
    }
  }
}

private[foundationdb] final case class MessageKey(subspace: Subspace, tuple: Tuple) extends Key {
  def persistenceId: String = tuple.getString(0)
  def sequenceNr: Long = tuple.getLong(1)
}

private[foundationdb] final case class MaxSequenceNrKey(subspace: Subspace, tuple: Tuple) extends Key {
  def persistenceId: String = tuple.getString(0)
}

private[foundationdb] final case class TagKey(subspace: Subspace, tuple: Tuple) extends VersionstampedKey {
  def tag: String = tuple.getString(0)
  def versionstamp: Versionstamp = tuple.getVersionstamp(1)
}

private[foundationdb] final case class TagWatchKey(subspace: Subspace, tuple: Tuple) extends Key

private[foundationdb] final case class SnapshotKey(subspace: Subspace, tuple: Tuple) extends Key {
  def persistenceId: String = tuple.getString(0)
  def sequenceNr: Long = tuple.getLong(1)
  def timestamp: Long = tuple.getLong(2)
}

private[foundationdb] class KeySerializer(directories: Directories) {
  def message(persistenceId: String, sequenceNr: Long): MessageKey = {
    MessageKey(directories.messages, Tuple.from(persistenceId, sequenceNr: java.lang.Long))
  }

  def maxSequenceNr(persistenceId: String): MaxSequenceNrKey = {
    MaxSequenceNrKey(directories.maxSeqNr, Tuple.from(persistenceId))
  }

  def tag(tag: String, versionstamp: Versionstamp): TagKey = {
    TagKey(directories.tags, Tuple.from(tag, versionstamp))
  }
  def tag(byteString: ByteString): TagKey = {
    val tuple = directories.tags.unpack(byteString.toArray)
    TagKey(directories.tags, tuple)
  }

  def tagWatch(tag: String): TagWatchKey = {
    TagWatchKey(directories.tagWatches, Tuple.from(tag))
  }
  def snapshot(persistenceId: String, sequenceNr: Long, timestamp: Long): SnapshotKey = {
    SnapshotKey(directories.snapshots, Tuple.from(persistenceId, sequenceNr: java.lang.Long, timestamp: java.lang.Long))
  }
}
