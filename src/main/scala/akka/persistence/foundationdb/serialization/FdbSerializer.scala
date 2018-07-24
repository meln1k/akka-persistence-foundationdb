package akka.persistence.foundationdb.serialization

import akka.persistence.PersistentRepr
import akka.persistence.foundationdb.{CompactTag, RichTag, TagType}
import akka.serialization.Serialization
import com.google.protobuf.ByteString

class FdbSerializer(serialization: Serialization) {

  private def persistentRepr2Bytes(p: PersistentRepr): Array[Byte] = serialization.serialize(p).get
  def bytes2PersistentRepr(bytes: Array[Byte]): PersistentRepr = serialization.deserialize(bytes, classOf[PersistentRepr]).get

  def serializePersistentRepr(persistentRepr: PersistentRepr): SerializedMessage = {
    SerializedMessage(
      persistentRepr.persistenceId,
      persistentRepr.sequenceNr,
      persistentRepr2Bytes(persistentRepr)
    )
  }

  def tagType2bytes(tag: TagType): Array[Byte] = {
    tag match {
      case CompactTag(persistenceId, sequenceNr) =>
        val compactBuilder = AkkaPersistenceFdb.CompactTag.newBuilder()
        .setPersistenceId(persistenceId)
        .setSequenceNr(sequenceNr)
        .build()

        AkkaPersistenceFdb.Tag.newBuilder()
          .setCompact(compactBuilder)
          .build()
          .toByteArray


      case RichTag(payload) =>
        val richBuilder = AkkaPersistenceFdb.RichTag.newBuilder()
        .setPayload(ByteString.copyFrom(payload))
        .build()

        AkkaPersistenceFdb.Tag.newBuilder()
          .setRich(richBuilder)
          .build()
          .toByteArray
    }
  }

  def bytes2TagType(bytes: Array[Byte]): TagType = {

    AkkaPersistenceFdb.Tag.parseFrom(bytes) match {
      case compact if compact.hasCompact =>
        CompactTag(compact.getCompact.getPersistenceId, compact.getCompact.getSequenceNr)
      case rich if rich.hasRich =>
        RichTag(rich.getRich.getPayload.toByteArray)
      case _ =>
        throw new IllegalArgumentException("tag must be either compact or rich!")
    }
  }

}
