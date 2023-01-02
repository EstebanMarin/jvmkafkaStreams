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

    // join

    val ordersWithUserProfiles: KStream[UserId, (Order, Profile)] =
      userOrderStreams.join(userProfileTable) {
        (order: Order, profile: Profile) => (order, profile)
      }

    val discountedOrderStreams: KStream[UserId, Order] =
      ordersWithUserProfiles.join(discountProfileGTable)(
        { case (userId, (order, profile)) => profile },
        { case ((order, profile), discount) =>
          order.copy(amount = order.amount - discount.amount)
        }
      )

    val orderStream: KStream[OrderId, Order] =
      discountedOrderStreams.selectKey((userId, order) => order.orderId)

    val paymentsTreams: KStream[OrderId, Payment] =
      builder.stream[OrderId, Payment](PaymentsTopic)

    val joinWindows = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))

    val joinOrderPayments: (Order, Payment) => Option[Order] =
      (order: Order, payment: Payment) =>
        if (payment.status == " PAID") Option(order) else Option.empty[Order]

    val ordersPaid1 = orderStream
      .join(paymentsTreams)(joinOrderPayments, joinWindows)
      .filter((orderId, mayberOrder) => mayberOrder.nonEmpty)

    val ordersPaid: KStream[OrderId, Order] = orderStream
      .join(paymentsTreams)(joinOrderPayments, joinWindows)
      .flatMapValues(maybeOrder => maybeOrder.toIterable)

    ordersPaid.to(PaidOrdersTopic)

    val topology: Topology = builder.build()

    val props = new Properties
    
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass())
    
    // println(topology.describe())
    val application = new KafkaStreams(topology, props)
    application.start()
  }

}
