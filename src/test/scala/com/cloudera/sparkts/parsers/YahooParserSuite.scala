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
package com.cloudera.sparkts.parsers

import java.time.ZoneId
import org.scalatest.FunSuite
import org.scalatest.Matchers.{be, convertToAnyShouldWrapper}

import java.io.InputStream
import scala.io.BufferedSource

class YahooParserSuite extends FunSuite {
  test("yahoo parser") {
    var cLoader: ClassLoader = null
    var ipStream: InputStream = null
    var buffS: BufferedSource = null
    try {
      cLoader = getClass.getClassLoader
      if (cLoader != null) {
        ipStream = cLoader.getResourceAsStream("GOOG.csv")
        buffS = scala.io.Source.fromInputStream(ipStream)
        if (buffS != null) {
          val lines = buffS.getLines().toArray
          val text = lines.mkString("\n")
          val ts = YahooParser.yahooStringToTimeSeries(text, zone = ZoneId.of("Z"))
          ts.data.numRows should be(lines.length - 1)
        }
      }
    } catch {
      case e: Exception => e.printStackTrace
    }
    finally {
      if (buffS != null) buffS.close()
      if (ipStream != null) ipStream.close()
    }
  }
}
