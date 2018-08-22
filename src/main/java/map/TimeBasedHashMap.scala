package map

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, Props}

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration}

trait TimeBasedHashMap[K, V]
{
  import akka.pattern.ask
  import MapActor._

  val system = ActorSystem("map")
  val actor = system.actorOf(Props(new MapActor[K,V]()))

  def put(k: K, v: V, duration: FiniteDuration): Future[Unit] = {
    actor.ask(Insert(k, v, duration)).mapTo[Unit]
  }
  def get(k: K): Future[Option[V]] = {
    actor.ask(Get(k)).mapTo[Option[V]]
  }
  def remove(k: K): Future[Option[V]] = {
    actor.ask(Remove(k)).mapTo[Option[V]]
  }

  def size(): Future[Int] = {
    actor.ask(GetSize).mapTo[Int]
  }
}

class MapActor[K, V] extends Actor {
  import MapActor._
  implicit val ctx = context.system.dispatcher

  val map = collection.mutable.Map[K, StampedObject[V]]()
  var counter: Long = 0

  override def receive: Receive = {
    case Insert(k: K, v: V, duration: FiniteDuration) =>
      map(k) = StampedObject(v, counter)
      counter += 1
      context.system.scheduler.scheduleOnce(duration) {self.tell(ConditionalRemove(k, counter - 1), self)}

    case GetSize =>
      sender().tell(map.size, self)

    case Remove(k: K) =>
      val result = map.remove(k)
      sender().tell(result.map(_.value), self)

    case ConditionalRemove(k: K, stamp: Long) =>
      map.get(k)
        .filter(stamped => stamped.stamp == stamp)
        .foreach(_ => map.remove(k))

    case Get(k: K) =>
      sender().tell(map.get(k), self)
  }
}

case class StampedObject[K](value: K, stamp: Long)

object MapActor {
  case class Insert[K, V](k: K, v: V, duration: Duration)
  case class Remove[K](k: K)
  case class Get[K](k: K)
  case class ConditionalRemove[K](k: K, stamp: Long)
  case object GetSize
}
