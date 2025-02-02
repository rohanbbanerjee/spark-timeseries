/**
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.sparkts.stats

import com.cloudera.sparkts.models.ARModel
import org.apache.spark.mllib.linalg.DenseVector
import org.scalatest.FunSuite

import java.security.SecureRandom

class AugmentedDickeyFullerSuite extends FunSuite {
  test("non-stationary AR model") {
    val rand = new SecureRandom()
    val arModel = new ARModel(0.0, .95)
    val sample = arModel.sample(500, rand)

    val (adfStat, pValue) = TimeSeriesStatisticalTests.adftest(sample, 1)
    assert(!java.lang.Double.isNaN(adfStat))
    assert(!java.lang.Double.isNaN(pValue))
    println("adfStat: " + adfStat)
    println("pValue: " + pValue)
  }

  test("iid samples") {
    val rand = new SecureRandom()
    val iidSample = Array.fill(500)(rand.nextDouble())
    val (adfStat, pValue) = TimeSeriesStatisticalTests.adftest(new DenseVector(iidSample), 1)
    assert(!java.lang.Double.isNaN(adfStat))
    assert(!java.lang.Double.isNaN(pValue))
    println("adfStat: " + adfStat)
    println("pValue: " + pValue)
  }
}
