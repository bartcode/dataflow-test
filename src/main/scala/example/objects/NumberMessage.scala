package example.objects

import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.joda.time.LocalDateTime

case class Identifier(number: Int)

trait NumberMessage[T <: NumberMessage[T]] extends Serializable {
  this: T =>

  def identified: (Identifier, T) = (Identifier(1), this)
}

//case class NumberMessage[T](timestamp: Option[LocalDateTime] = None, number: Option[Int] = None) {
//  def identified[T]: SCollection[Identifier, NumberMessage[T]] = SCollection[Identifier(1), this]
//}
//
//object NumberMessage {
//  def apply[T](o: PubsubMessage): NumberMessage[T] = {
//    new NumberMessage()
//  }
//}
