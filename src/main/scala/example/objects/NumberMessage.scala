package example.objects

import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.joda.time.LocalDateTime

case class Identifier(number: Option[Int])

trait NumberMessage[T <: NumberMessage[T]] extends Serializable {
  this: T =>

  implicit private val identifier: Option[Int] = None

  def identified: (Identifier, T) = (Identifier(identifier), this)
}
