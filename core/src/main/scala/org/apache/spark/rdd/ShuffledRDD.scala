/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag
import scala.util.Sorting

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer



private[spark] class ShuffledRDDPartition(val idx: Int) extends Partition {
  override val index: Int = idx
}

/**
 * :: DeveloperApi ::
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 * @param prev the parent RDD.
 * @param part the partitioner used to partition the RDD
 * @tparam K the key class.
 * @tparam V the value class.
 * @tparam C the combiner class.
 */
// TODO: Make this return RDD[Product2[K, C]] or have some way to configure mutable pairs
@DeveloperApi
class ShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient var prev: RDD[_ <: Product2[K, V]],
    part: Partitioner)
  extends RDD[(K, C)](prev.context, Nil) {

  private var userSpecifiedSerializer: Option[Serializer] = None

  private var keyOrdering: Option[Ordering[K]] = None

  private var aggregator: Option[Aggregator[K, V, C]] = None

  private var mapSideCombine: Boolean = false

  opted = prev.opted

  val executorLocationTag = "executor_"

  // optimized prefered locations
  private var optLocationPrefs = new HashMap[Int, Seq[String]]()

  private var prefLocUpdated = false

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): ShuffledRDD[K, V, C] = {
    this.userSpecifiedSerializer = Option(serializer)
    this
  }

  /** Set key ordering for RDD's shuffle. */
  def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledRDD[K, V, C] = {
    this.keyOrdering = Option(keyOrdering)
    this
  }

  /** Set aggregator for RDD's shuffle. */
  def setAggregator(aggregator: Aggregator[K, V, C]): ShuffledRDD[K, V, C] = {
    this.aggregator = Option(aggregator)
    this
  }

  /** Set mapSideCombine flag for RDD's shuffle. */
  def setMapSideCombine(mapSideCombine: Boolean): ShuffledRDD[K, V, C] = {
    this.mapSideCombine = mapSideCombine
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    val serializer = userSpecifiedSerializer.getOrElse {
      val serializerManager = SparkEnv.get.serializerManager
      if (mapSideCombine) {
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[C]])
      } else {
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[V]])
      }
    }
    List(new ShuffleDependency(prev, part, serializer, keyOrdering, aggregator, mapSideCombine))
  }

  override val partitioner = Some(part)

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))
  }

  override protected def getPreferredLocations(partition: Partition): Seq[String] = {
    if (opted) {
      if (prefLocUpdated) {
        optLocationPrefs.getOrElse(partition.index, Nil)
      } else {
        updatePrefLoc()
        prefLocUpdated = true
        optLocationPrefs.getOrElse(partition.index, Nil)
      }
    } else {
      val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
      val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
      tracker.getPreferredLocationsForShuffle(dep, partition.index)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }

  private def updatePrefLoc(): Unit = {
    val sc = context

    val availableArray = sc.workloadDiv(sc.executorTokens.size())

    // after the array is sorted, assign each partition to an availabe executor
    for ((pair, partID) <- availableArray.zipWithIndex) {
      val execID = pair._1
      val execHost = executorLocationTag + s"${sc.executorToHost.get(execID)}_$execID"
      if (optLocationPrefs.contains(partID)) {
        optLocationPrefs = optLocationPrefs.updated(
          partID, optLocationPrefs(partID) :+ execHost)
      } else {
        optLocationPrefs += (partID -> Seq(execHost))
      }
    }
  }
}
