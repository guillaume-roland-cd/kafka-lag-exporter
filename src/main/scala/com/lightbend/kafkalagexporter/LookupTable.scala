/*
 * Copyright (C) 2018-2022 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2022 Sean Glover <https://seanglover.com>
 */

package com.lightbend.kafkalagexporter

import java.time.Clock

import com.lightbend.kafkalagexporter.ConsumerGroupCollector.CollectorConfig
import com.redis.RedisClient

import scala.collection.mutable
import scala.util.control._

object LookupTable {

  import Domain._
  import Table._

  case class Point(offset: Long, time: Long)

  /**
    * Try to convert a String to a Long. Return Either a Long if conversion succeed or None if a NumberFormatException is raised
    */
  def toLong(s: String): Option[Long] = {
    try {
      Some(s.toLong)
    } catch {
      case _: NumberFormatException => None
    }
  }

  /**
    * linear interpolation, solve for the x intercept given y (val), slope (dy/dx), and starting point (right)
    */
  def predict(offset: Long,
              left: Point,
              right: Point): Result = {
    val dx = (right.time - left.time).toDouble
    val dy = (right.offset - left.offset).toDouble
    val Px = right.time.toDouble
    val Dy = (right.offset - offset).toDouble
    Prediction(Px - Dy * dx / dy)
  }

  case class RedisTable(tp: TopicPartition, config: CollectorConfig) {
    val pointsKey: String = config.redis.prefix + config.redis.separator + config.cluster.name + config.redis.separator + tp.topic + config.redis.separator + tp.partition + config.redis.separator + "points"
    val lastUpdatedKey: String = config.redis.prefix + config.redis.separator + config.cluster.name + config.redis.separator + tp.topic + config.redis.separator + tp.partition + config.redis.separator + "updated"

    /**
     * Add the `Point` to the table.
     */
    def addPoint(point: Point,
                 redisClient: RedisClient,
                 currentTimestamp: Long = Clock.systemUTC().instant().toEpochMilli): PointResult = {
      mostRecentPoint(redisClient) match {
        // new point is out of order
        case Right(mrp) if mrp.time >= point.time => OutOfOrder
        // new point is not part of a monotonically increasing set
        case Right(mrp) if mrp.offset > point.offset => NonMonotonic
        // compress flat lines to a single segment
        case Right(mrp) if mrp.offset == point.offset &&
          length(redisClient) > 1 &&
          mostRecentPoint(redisClient, 1, 1).right.get.offset == point.offset =>
          // update the most recent point
          removeExpiredPoints(redisClient)
          val times = redisClient.zrangebyscore(key = pointsKey, min = point.offset, max = point.offset, limit = Some((0, 2): (Int, Int))).get
          redisClient.zremrangebyscore(key = pointsKey, start = point.offset, end = point.offset)
          redisClient.zadd(pointsKey, point.offset.toDouble, times.minBy(_.toLong))
          redisClient.zadd(pointsKey, point.offset.toDouble, point.time)
          expireKeys(redisClient)
          UpdatedSameOffset
        case Right(mrp) =>
          // dequeue oldest point if we've hit the limit
          removeExpiredPoints(redisClient)
          val lastUpdatedTimestamp = redisClient.get(lastUpdatedKey).getOrElse("0")
          toLong(lastUpdatedTimestamp) match {
            case Some(lastUpdatedTimestamp) if currentTimestamp - lastUpdatedTimestamp < config.redis.resolution.toMillis =>
              redisClient.zremrangebyscore(key = pointsKey, start = mrp.offset, end = mrp.offset)
              redisClient.zadd(pointsKey, point.offset.toDouble, point.time)
              expireKeys(redisClient)
              UpdatedRetention
            case _ =>
              redisClient.zadd(pointsKey, point.offset.toDouble, point.time)
              redisClient.set(lastUpdatedKey, currentTimestamp.toString)
              expireKeys(redisClient)
              Inserted
          }
        // the table is empty or we filtered thru previous cases on the most recent point
        case _ =>
          // dequeue oldest point if we've hit the limit
          removeExpiredPoints(redisClient)
          redisClient.zadd(pointsKey, point.offset.toDouble, point.time)
          redisClient.set(lastUpdatedKey, currentTimestamp.toString)
          expireKeys(redisClient)
          Inserted
      }
    }

    /**
     * Expire keys in Redis with the configured TTL
     */
    def expireKeys(redisClient: RedisClient): Unit = {
      redisClient.expire(pointsKey, config.redis.expiration.toSeconds.toInt)
      redisClient.expire(lastUpdatedKey, config.redis.expiration.toSeconds.toInt)
    }

    /**
     * Remove points that are older than the configured retention
     */
    def removeExpiredPoints(redisClient: RedisClient): Unit = {
      val currentTimestamp: Long = Clock.systemUTC().instant().toEpochMilli
      val loop = new Breaks
      loop.breakable {
        for (_ <- (0: Long) until length(redisClient)) {
          oldestPoint(redisClient) match {
            case Left(_) => loop.break() // No Data
            case Right(p) =>
              if (currentTimestamp - config.redis.retention.toMillis > p.time) removeOldestPoint(redisClient)
              else loop.break()
          }
        }
      }
    }

    /**
     * Remove the oldest point
     */
    def removeOldestPoint(redisClient: RedisClient): Unit = {
      redisClient.zremrangebyrank(pointsKey, 0, 0)
    }

    /**
     * Predict the timestamp of a provided offset using interpolation if in the sliding window, or extrapolation if outside the sliding window.
     */
    def lookup(offset: Long,
               redisClient: RedisClient): Result = {
      def estimate(): Result = {
        // Look in the sorted set for an exact point. It can happens by chance or during flattened range
        redisClient zrangebyscore(key = pointsKey, min = offset.toDouble, max = offset.toDouble, limit = Some((0, 1): (Int, Int)), sortAs = RedisClient.DESC) match {
          // unexpected situation where the redis result is None
          case None => return RedisNone
          case Some(points) if points.nonEmpty => return Prediction(points.head.toDouble)
          case Some(_) => // Exact point not found, moving to range calculation
        }

        var left: Either[String, Point] = redisClient.zrangebyscoreWithScore(key = pointsKey, min = 0, max = offset.toDouble, maxInclusive = false , limit = Some((0, 1): (Int, Int)), sortAs = RedisClient.DESC) match {
          // unexpected situation where the redis result is None
          case None => return RedisNone
          // find the left Point from the offset
          case Some(lefts) if lefts.nonEmpty => Right(Point(lefts.head._2.toLong, lefts.head._1.toLong))
          // offset is not between any two points in the table
          case _ =>
            //extrapolated = true
            Left("Extrapolation required")
        }

        var right: Either[String, Point] = redisClient.zrangebyscoreWithScore(key = pointsKey, min = offset.toDouble, minInclusive = false, max = Double.PositiveInfinity, limit = Some((0, 1): (Int, Int)), sortAs = RedisClient.ASC) match {
          // unexpected situation where the redis result is None
          case None => return RedisNone
          // find the right Point from the offset
          case Some(rights) if rights.nonEmpty || left.isLeft => Right(Point(rights.head._2.toLong, rights.head._1.toLong))
          // offset is not between any two points in the table
          case _ =>
            //extrapolated = true
            Left("Extrapolation required")
        }

        // extrapolate given largest trendline we have available
        if (left.isLeft || right.isLeft) {
          left = oldestPoint(redisClient)
          right = mostRecentPoint(redisClient)
        }

        if (left.isRight && right.isRight) {
          predict(offset, left.right.get, right.right.get)
        } else {
          TooFewPoints
        }
      }

      mostRecentPoint(redisClient) match {
        case Right(mrp) if mrp.offset == offset => LagIsZero
        case _ if length(redisClient) < 2 => TooFewPoints
        case _ => estimate()
      }
    }

    /**
     * Return the oldest `Point`.  Returns either an error message, or the `Point`.
     */
    def oldestPoint(redisClient: RedisClient, start: Int = 0, end: Int = 0): Either[String, Point] = {
      val r = redisClient.zrangeWithScore(key = pointsKey, start = start, end = end, sortAs = RedisClient.ASC).get
      if (r.isEmpty) Left("No data in redis")
      else Right(Point(r.head._2.toLong, r.head._1.toLong))
    }

    /**
     * Return the size of the lookup table.
     */
    def length(redisClient: RedisClient): Long = {
      redisClient.zcount(pointsKey).getOrElse(0)
    }

    /**
     * Return the most recently added `Point`.  Returns either an error message, or the `Point`.
     */
    def mostRecentPoint(redisClient: RedisClient, start: Int = 0, end: Int = 0): Either[String, Point] = {
      val r = redisClient.zrangeWithScore(key = pointsKey, start = start, end = end, sortAs = RedisClient.DESC).get
      if (r.isEmpty) Left("No data in redis")
      else Right(Point(r.head._2.toLong, r.head._1.toLong))
    }
  }

  case class MemoryTable(limit: Int, points: mutable.Queue[Point]) {

    /** Add the `Point` to the table.
      */
    def addPoint(point: Point): Unit = mostRecentPoint() match {
      // new point is out of order
      case Right(mrp) if mrp.time >= point.time =>
      // new point is not part of a monotonically increasing set
      case Right(mrp) if mrp.offset > point.offset =>
      // compress flat lines to a single segment
      // rather than run the table into a flat line, just move the right hand side out until we see variation again
      // Ex)
      //   Table contains:
      //     Point(offset = 200, time = 1000)
      //     Point(offset = 200, time = 2000)
      //   Add another entry for offset = 200
      //     Point(offset = 200, time = 3000)
      //   If we still have not incremented the offset, replace the last entry.  Table now looks like:
      //     Point(offset = 200, time = 1000)
      //     Point(offset = 200, time = 3000)
      case Right(mrp)
          if mrp.offset == point.offset &&
            points.length > 1 &&
            points(points.length - 2).offset == point.offset =>
        // update the most recent point
        points(points.length - 1) = point
      // the table is empty or we filtered thru previous cases on the most recent point
      case _ =>
        // dequeue oldest point if we've hit the limit (sliding window)
        if (points.length == limit) points.dequeue()
        points.enqueue(point)
    }

    /** Predict the timestamp of a provided offset using interpolation if in the
      * sliding window, or extrapolation if outside the sliding window.
      */
    def lookup(offset: Long): Result = {
      def estimate(): Result = {
        // search for two cells that contains the given offset
        val (left, right) = points.reverseIterator
          // create a sliding window of 2 elements with a step size of 1
          .sliding(size = 2, step = 1)
          // convert window to a tuple. since we're iterating backwards we match right and left in reverse.
          .map { case r :: l :: Nil => (l, r) }
          // find the Point that contains the offset
          .find { case (l, r) => offset >= l.offset && offset <= r.offset }
          // offset is not between any two points in the table
          // extrapolate given largest trendline we have available
          .getOrElse {
            (points.head, points.last)
          }

        predict(offset, left, right)
      }

      points.toList match {
        // if last point is same as looked up group offset then lag is zero
        case p if p.nonEmpty && offset == p.last.offset => LagIsZero
        case p if p.length < 2                          => TooFewPoints
        case _                                          => estimate()

      }
    }

    /** Return the most recently added `Point`. Returns either an error message,
      * or the `Point`.
      */
    def mostRecentPoint(): Either[String, Point] = {
      if (points.isEmpty) Left("No data in table")
      else Right(points.last)
    }
  }

  object Table {
    def apply(tp: TopicPartition, config: CollectorConfig): Either[MemoryTable, RedisTable] = {
      if (config.redis.enabled)
        Right(RedisTable(tp, config))
      else
        Left(MemoryTable(config.lookupTableSize, mutable.Queue[Point]()))
    }

    def apply(limit: Int): MemoryTable = MemoryTable(limit, mutable.Queue[Point]())
    sealed trait Result
    case object TooFewPoints extends Result
    case object LagIsZero extends Result
    case object RedisNone extends Result
    sealed trait PointResult
    case object OutOfOrder extends PointResult
    case object NonMonotonic extends PointResult
    case object UpdatedSameOffset extends PointResult
    case object Inserted extends PointResult
    case object UpdatedRetention extends PointResult
    final case class Prediction(time: Double) extends Result
  }
}
