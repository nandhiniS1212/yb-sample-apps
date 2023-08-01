
// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

package com.yugabyte.sample.apps;

//import static com.yugabyte.sample.apps.UnpreparedCassandraKeyValueBase.LOG;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

//import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.yugabyte.sample.common.SimpleLoadGenerator.Key;

/**
 * This workload writes and reads some random string keys from a CQL server. By default, this app
 * inserts a million keys, and reads/updates them indefinitely.
 */
public class UnpreparedCassandraKeyValue extends UnpreparedCassandraKeyValueBase {
  private static final Logger LOG = Logger.getLogger(UnpreparedCassandraKeyValue.class);
  // The default table name to create and use for CRUD ops.
  private static final String DEFAULT_TABLE_NAME = CassandraKeyValue.class.getSimpleName();
  static {
    // The number of keys to read.
    appConfig.numKeysToRead = NUM_KEYS_TO_READ_FOR_YSQL_AND_YCQL;
    // The number of keys to write. This is the combined total number of inserts and updates.
    appConfig.numKeysToWrite = NUM_KEYS_TO_WRITE_FOR_YSQL_AND_YCQL;
    // The number of unique keys to write. This determines the number of inserts (as opposed to
    // updates).
    appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS_FOR_YSQL_AND_YCQL;
  }
  @Override
  public List<String> getCreateTableStatements() {
    String create_stmt = String.format(
      "CREATE TABLE IF NOT EXISTS %s (k varchar, v blob, primary key (k))", getTableName());

    if (appConfig.tableTTLSeconds > 0) {
      create_stmt += " WITH default_time_to_live = " + appConfig.tableTTLSeconds;
    }
    create_stmt += ";";
    return Arrays.asList(create_stmt);
  }
  //private static final Object unpreparedInitLock = new Object();
  @Override
  protected String getDefaultTableName() {
    return DEFAULT_TABLE_NAME;
   }

  @Override
  
  protected String queryInsert(String key, ByteBuffer value) {
    //synchronized (unpreparedInitLock) {
   // String vall = new String(value.array());

   // String val = Charset.forName("UTF-8").decode(value).toString();
    //String val = StandardCharsets.UTF_8.decode(value).toString();
    // LOG.info("key: " + key + "    Value: " + bytesToHex(value));
    return String.format("INSERT INTO %s (k, v) VALUES ('%s', 0x%s);", getTableName(), key, bytesToHex(value));
    //}
  }

  //in unprepared statements you directly query without bind 
  @Override 
  protected String querySelect(String key)  {
    //synchronized (unpreparedInitLock) {
    return  String.format("SELECT k, v FROM %s WHERE k = '%s';", getTableName(),key);
   // }
  }

  @Override
  public List<String> getWorkloadDescription() {
    return Arrays.asList(
      "Sample key-value app built on Cassandra with concurrent reader and writer threads.",
      " Each of these threads operates on a single key-value pair. The number of readers ",
      " and writers, the value size, the number of inserts vs updates are configurable. " ,
       "By default number of reads and writes operations are configured to "+AppBase.appConfig.numKeysToRead+" and "+AppBase.appConfig.numKeysToWrite+" respectively." ,
       " User can run read/write(both) operations indefinitely by passing -1 to --num_reads or --num_writes or both");
  }

  public static String bytesToHex(ByteBuffer temp) {
    // String temp2 = new String(temp.array());
    // LOG.info("------------------------------------------ " + temp2);
    ByteBuffer byteBuffer = temp;
    StringBuilder hexString = new StringBuilder();
    while (byteBuffer.hasRemaining()) {
        String hex = Integer.toHexString(byteBuffer.get() & 0xFF);
        if (hex.length() == 1) {
            hexString.append('0');
        }
        hexString.append(hex);
    }
    // String temp3 = new String(temp.array());
    // LOG.info("+++++++++++++++++++++++++++++++++++++++++++ " + temp3);
    return hexString.toString();
  }
  public static String bytesToDecimal(ByteBuffer byteBuffer) {
    StringBuilder decimalString = new StringBuilder();
    while (byteBuffer.hasRemaining()) {
        int unsignedValue = byteBuffer.get() & 0xFF; // Convert signed byte to unsigned int
        decimalString.append(unsignedValue).append(" ");
    }
    return decimalString.toString().trim();
  }
}
