package example.objects

import org.joda.time.{DateTime, Instant}
import example.proto.message.NumberBuffer

case class NumberInfo(id: Option[Long],
                      timestamp: Option[Instant],
                      name: Option[String],
                      number: Option[Long],
                      `type`: Option[String])
  extends NumberMessage[NumberInfo]

object NumberInfo {
  def apply(buffer: NumberBuffer): NumberInfo = {
    new NumberInfo(
      id = Some(buffer.id),
      timestamp = Some(new DateTime(buffer.timestamp).toInstant),
      name = Some(buffer.name),
      number = Some(buffer.number),
      `type` = Some(buffer.`type`)
    )
  }
}
