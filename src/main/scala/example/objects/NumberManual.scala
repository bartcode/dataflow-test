package example.objects

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.joda.time.LocalDateTime

case class NumberManual(timestamp: Option[LocalDateTime] = None, number: Option[Int] = None)
  extends NumberMessage[NumberManual] {

}

object NumberManual {
  def apply(o: PubsubMessage): NumberManual = {
    new NumberManual()
  }
}
