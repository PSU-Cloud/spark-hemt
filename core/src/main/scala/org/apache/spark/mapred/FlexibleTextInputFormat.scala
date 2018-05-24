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

package org.apache.spark.mapred

import org.apache.hadoop.fs.{FileStatus, FileSystem, LocatedFileStatus}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, JobConf, TextInputFormat}
import org.apache.hadoop.net.NetworkTopology

import org.apache.spark.internal.Logging

/**
 * Unlike TextInputFormat that is used in original Spark, this flexible InputFormat allows:
 * 1. a split whose size is larger than block size;
 * 2. splitting a file according to a certain array of weights - splitWeights.
 */
class FlexibleTextInputFormat extends TextInputFormat with Logging {
  private var splitWeights = Array[Double]()
  logWarning("FlexibleTextInputFormat is used.")

  def updateSplitWeights(weights: Array[Double]): Unit = {
    splitWeights = weights
  }

  override def getSplits(job: JobConf, numSplits: Int): Array[InputSplit] = {
    if (splitWeights.length == 0) {
      super.getSplits(job, numSplits)
    } else {
      val proportions = splitWeights.map(_ / splitWeights.sum)
      val files = listStatus(job)
      assert(files.length <= 1, "Only support single file!")
      val clusterMap = new NetworkTopology()
      // var totalSize = files.map(_.getLen).sum
      var splits = Array[FileSplit]()
      for (file <- files) {
        val path = file.getPath
        val length = file.getLen
        if (length != 0) {
          val fs = path.getFileSystem(job)
          val blkLocations = if (file.isInstanceOf[LocatedFileStatus]) {
            file.asInstanceOf[LocatedFileStatus].getBlockLocations
          } else {
            fs.getFileBlockLocations(file, 0, length)
          }
          if (isSplitable(fs, path)) {
            var accLen: Long = 0
            for (i <- 0 until proportions.length - 1) {
              val splitSize = (length * proportions(i)).toLong
              // TODO(yuquanshan): currently we don't consider the hosts with this file cached
              // need to consider them in the future,
              // see FileInputFormat.getSplitHostsAndCachedHosts
              val splitHosts = getSplitHosts(blkLocations, accLen, splitSize, clusterMap)
              splits = splits :+ makeSplit(path, accLen, splitSize, splitHosts)
              logWarning(s"Split $splitSize out.")
              accLen += splitSize
            }
            val splitHosts = getSplitHosts(blkLocations, accLen, length - accLen, clusterMap)
            splits = splits :+ makeSplit(path, accLen, length - accLen, splitHosts)
            logWarning(s"Split ${length - accLen} out.")
          } else {
            val splitHosts = getSplitHosts(blkLocations, 0, length, clusterMap)
            splits = splits :+ makeSplit(path, 0, length, splitHosts)
          }
        } else {
          splits = splits :+ makeSplit(path, 0, length, Array[String]())
        }
      }
      splits.map(_.asInstanceOf[InputSplit])
    }
  }
}