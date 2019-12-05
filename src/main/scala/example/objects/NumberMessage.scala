package example.objects


case class Identifier(number: Option[Int])

trait NumberMessage[T <: NumberMessage[T]] extends Serializable {
  this: T =>

  implicit private val identifier: Option[Int] = None

  def identified: (Identifier, T) = (Identifier(identifier), this)
}
