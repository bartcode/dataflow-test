package example.objects

import java.time.ZoneOffset

import example.message.NumberBuffer
import java.time.{Instant, LocalDateTime}

case class NumberInfo(timestamp: Option[LocalDateTime] = None,
                      number: Option[Int] = None,
                      `type`: Option[String],
                      name: Option[String])
  extends NumberMessage[NumberInfo] {

}

object NumberInfo {
  def apply(buffer: NumberBuffer): NumberInfo = {
    new NumberInfo(
      timestamp = Some(LocalDateTime.ofInstant(Instant.ofEpochSecond(buffer.timestamp), ZoneOffset.UTC)),
      number = Some(buffer.number),
      `type` = Some(buffer.`type`),
      name = Some(buffer.name)
    )
  }
}