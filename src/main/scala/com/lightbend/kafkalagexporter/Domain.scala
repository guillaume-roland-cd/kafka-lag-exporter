/*
 * Copyright (C) 2019-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package com.lightbend.kafkalagexporter

object Domain {
  final case class TopicPartition(topic: String, partition: Int)

  // Once upon a time I had a nested data structure for all these types, but I found that a flat datastructure
  // is much easier to work with, at the sacrifice of some duplication.
  final case class GroupTopicPartition(
                                        id: String,
                                        clientId: String,
                                        consumerId: String,
                                        host: String,
                                        topic: String,
                                        partition: Int) {
    lazy val tp: TopicPartition = TopicPartition(topic, partition)
  }

  type GroupOffsets = Map[GroupTopicPartition, Option[LookupTable.Point]]

  object GroupOffsets {
    def apply(): GroupOffsets = Map.empty[GroupTopicPartition, Option[LookupTable.Point]]
    def apply(tuples: (GroupTopicPartition, Option[LookupTable.Point])*): GroupOffsets = Map(tuples: _*)
  }

  type PartitionOffsets = Map[TopicPartition, LookupTable.Point]

  object PartitionOffsets {
    def apply(): PartitionOffsets = Map.empty[TopicPartition, LookupTable.Point]
    def apply(tuples: (TopicPartition, LookupTable.Point)*): PartitionOffsets = Map(tuples: _*)
  }

  class TopicPartitionTable private(var tables: Map[TopicPartition, Either[LookupTable.MemoryTable, LookupTable.RedisTable]], config: ConsumerGroupCollector.CollectorConfig) {
    def apply(tp: TopicPartition): Either[LookupTable.MemoryTable, LookupTable.RedisTable] = {
      tables = tables.updated(tp, tables.getOrElse(tp, LookupTable.Table(tp, config)))
      tables(tp)
    }

    def clear(evictedTps: List[TopicPartition]): Unit =
      tables = tables.filterKeys(tp => !evictedTps.contains(tp))

    def all: Map[TopicPartition, Either[LookupTable.MemoryTable, LookupTable.RedisTable]] = tables
  }

  object TopicPartitionTable {
    def apply(tables: Map[TopicPartition, Either[LookupTable.MemoryTable, LookupTable.RedisTable]] = Map.empty[TopicPartition, Either[LookupTable.MemoryTable, LookupTable.RedisTable]],
              config: ConsumerGroupCollector.CollectorConfig): TopicPartitionTable =
      new TopicPartitionTable(tables, config)
  }
}
