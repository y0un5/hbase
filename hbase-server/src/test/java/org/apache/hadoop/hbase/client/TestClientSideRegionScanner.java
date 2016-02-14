/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({SmallTests.class, ClientTests.class})
public class TestClientSideRegionScanner {
  @Rule public TestName name = new TestName();

  private final static HBaseTestingUtility HTU = HBaseTestingUtility.createLocalHTU();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HTU.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    HTU.shutdownMiniCluster();
  }

  @Test
  public void testScan() throws Exception {
    Configuration conf = HTU.getConfiguration();
    Path rootRegionDir = HTU.getDataTestDirOnTestFS(name.getMethodName());
    FileSystem fs = rootRegionDir.getFileSystem(conf);

    TableName tableName = TableName.valueOf(name.getMethodName());
    HRegionInfo hri = new HRegionInfo(tableName, null, null, false);
    //HTableDescriptor htd = HTU.createTableDescriptor(tableName);
    HTableDescriptor htd = HTU.createTableDescriptor(tableName.getNameAsString());
    //HRegion region = HTU.createRegionAndWAL(hri, rootRegionDir, conf, htd);
    HRegion region = HRegion.createHRegion(hri, rootRegionDir, conf, htd);

    Put put = new Put("r".getBytes());
    for (int i = 0; i < 10; i++) {
      //put.addColumn(HTU.fam1, ("q" + i).getBytes(), "v".getBytes());
      put.add(HTU.fam1, ("q" + i).getBytes(), "v".getBytes());
    }
    region.put(put);
    //HTU.closeRegionAndWAL(region);
    HRegion.closeHRegion(region);

    Scan scan = new Scan();
    scan.setBatch(4);

    ClientSideRegionScanner scanner =
      new ClientSideRegionScanner(conf, fs, rootRegionDir, htd, hri, scan, null);

    Result result = scanner.next();
    do {
      System.out.println("result: " + result);
      System.out.println("result.size(): " + result.size());
    } while ((result = scanner.next()) != null);
    scanner.close();
  }
}
