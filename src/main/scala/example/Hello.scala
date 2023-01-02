package example

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{
  GlobalKTable,
  JoinWindows,
  TimeWindows,
  Windowed
}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties
import scala.concurrent.duration._

object Hello {
  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String

    case class Order(
        orderId: OrderId,
        user: UserId,
        products: List[Product],
        amount: Double
    )
    case class Discount(
        profile: Profile,
        amount: Double
    ) // in percentage points
    case class Payment(orderId: OrderId, status: String)
  }

  object Topics {
    final val OrdersByUserTopic = "orders-by-user"
    final val DiscountProfilesByUserTopic = "discount-profiles-by-user"
    final val DiscountsTopic = "discounts"
    final val OrdersTopic = "orders"
    final val PaymentsTopic = "payments"
    final val PaidOrdersTopic = "paid-orders"
  }

  def main(args: Array[String]): Unit = {
    // List(
    //   "orders-by-user",
    //   "discount-profiles-by-user",
    //   "discounts",
    //   "orders",
    //   "payments",
    //   "paid-orders"
    // ).foreach { topic =>
    //   println(
    //     s"kafka-topics --bootstrap-server localhost:9092 --topic ${topic} --create"
    //   )
    // }

    import Domain._
    import Topics._

    implicit def serdeOrder[A >: Null: Decoder: Encoder]: Serde[A] = {
      val serializer: A => Array[Byte] = (a: A) => a.asJson.noSpaces.getBytes()
      val deserializer: Array[Byte] => Option[A] = (bytes: Array[Byte]) => {
        val string = new String(bytes)
        decode[A](string).toOption
      }

      Serdes.fromFn[A](serializer, deserializer)
    }

    // topology

    val builder = new StreamsBuilder()

    // KStreams
    val userOrderStreams: KStream[UserId, Order] =
      builder.stream[UserId, Order](OrdersByUserTopic)

    // Ktable
    val userProfileTable: KTable[UserId, Profile] =
      builder.table[UserId, Profile](DiscountProfilesByUserTopic)

    // GlobalKtable
    val discountProfileGTable: GlobalKTable[Profile, Discount] =
      builder.globalTable[Profile, Discount](DiscountsTopic)

    val expensiveOrders: KStream[UserId, Order] = userOrderStreams.filter {
      (userId, orders) =>
        orders.amount > 1000
    }

    val listOrProducts: KStream[UserId, List[Product]] =
      userOrderStreams.mapValues(order => order.products)

    val productStream: KStream[UserId, Product] =
      userOrderStreams.flatMapValues(_.products)

    builder.build()

    println("HELLO WORLD")
  }

}
