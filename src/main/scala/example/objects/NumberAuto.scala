package example.objects

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.joda.time.LocalDateTime

case class NumberAuto(timestamp: Option[LocalDateTime] = None, number: Option[Int] = None)
  extends NumberMessage[NumberAuto] {

}

object NumberAuto {
  def apply(o: PubsubMessage): NumberAuto = {
    new NumberAuto()
  }
}
