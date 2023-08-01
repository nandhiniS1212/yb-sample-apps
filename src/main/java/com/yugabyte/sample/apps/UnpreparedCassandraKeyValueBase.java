
package com.yugabyte.sample.apps;

import com.datastax.driver.core.*;
import com.yugabyte.sample.common.SimpleLoadGenerator;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
//import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for all workloads that are based on key value tables.
 */
public abstract class UnpreparedCassandraKeyValueBase extends AppBase {
  private static final Logger LOG = Logger.getLogger(UnpreparedCassandraKeyValueBase.class);
  private static final Object unpreparedInitLock = new Object();
  private static volatile String unpreparedSelect;

  // The shared prepared statement for inserting into the table.
  private static volatile String unpreparedInsert;
  //private static final Object unpreparedInitLock = new Object();
  // Static initialization of this workload's config. These are good defaults for getting a decent
  // read dominated workload on a reasonably powered machine. Exact IOPS will of course vary
  // depending on the machine and what resources it has to spare.
  static {
    // Disable the read-write percentage.
    appConfig.readIOPSPercentage = -1;
    // Set the read and write threads to 1 each.
    appConfig.numReaderThreads = 24;
    appConfig.numWriterThreads = 2;
    // The number of keys to read.
    appConfig.numKeysToRead = 1500000;
    // The number of keys to write. This is the combined total number of inserts and updates.
    appConfig.numKeysToWrite = 2000000;
    // The number of unique keys to write. This determines the number of inserts (as opposed to
    // updates).
    appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS;
  }

  // The shared unprepared select statement for fetching the data.
 // private static volatile Statement unpreparedSelect;

  // The shared unprepared statement for inserting into the table.
  //private static volatile Statement unpreparedInsert;

  public UnpreparedCassandraKeyValueBase() {
    buffer = new byte[appConfig.valueSize];
  }

  /**
   * Drop the table created by this app.
   */
  @Override
  public void dropTable() {
    dropCassandraTable(getTableName());
  }

  protected abstract String getDefaultTableName();

  public String getTableName() {
    return appConfig.tableName != null ? appConfig.tableName : getDefaultTableName();
  }

  @Override
  public synchronized void resetClients() {
    synchronized (unpreparedInitLock) {
        unpreparedInsert = null;
        unpreparedSelect = null;
      }
    super.resetClients();
  }

  @Override
  public synchronized void destroyClients() {
    synchronized (unpreparedInitLock) {
        unpreparedInsert = null;
        unpreparedSelect = null;
      }
    super.destroyClients();
  }

  protected abstract String querySelect(String key);
  protected abstract String queryInsert(String key, ByteBuffer value);

  protected String getUnpreparedSelect(String key)  {
    // String unpreparedSelectLocal = unpreparedSelect;
    // if (unpreparedSelectLocal == null) {
      synchronized (unpreparedInitLock) {
     //   if (unpreparedSelect == null) {
          unpreparedSelect = querySelect(key);
        //   if (localReads) {
        //     LOG.debug("Doing local reads");
        //     //unpreparedSelect.setConsistencyLevel(ConsistencyLevel.ONE);
        //   }
        }
    //     unpreparedSelectLocal = unpreparedSelect;
    //   }
    //}
    return unpreparedSelect;
  }

  protected String getUnpreparedInsert(String key, ByteBuffer value)  {
    String unpreparedInsertLocal = unpreparedInsert;
    if (unpreparedInsert == null) {
      synchronized (unpreparedInitLock) {
        if (unpreparedInsert == null) {
          // Create the prepared statement object.
          unpreparedInsert = queryInsert(key, value);
        }
        unpreparedInsertLocal = unpreparedInsert;
      }
    }
    return unpreparedInsertLocal;
  }


  @Override
    public long doRead() {
        SimpleLoadGenerator.Key key = getSimpleLoadGenerator().getKeyToRead();
        if (key == null) {
            // There are no keys to read yet.
            return 0;
        }
        // Do the read from Cassandra.
        //synchronized (unpreparedInitLock) {
            //if (unpreparedSelect == null) {
                String select  = querySelect(key.asString());   
                ResultSet rs = getCassandraClient().execute(select);
            //}

                List<Row> rows = rs.all();
                if (rows.size() != 1) {
                    // If TTL is enabled, turn off correctness validation.
                    if (appConfig.tableTTLSeconds <= 0) {
                    LOG.fatal("Read key: " + key.asString() + " expected 1 row in result, got " + rows.size());
                    }
                    return 1;
                }
                if (appConfig.valueSize == 0) {
                    ByteBuffer buf = rows.get(0).getBytes(1);
                    String value = new String(buf.array());
                    //LOG.info("Bytebuffer size: " + buf.capacity() + " value: " + value + " value size: " + value.length());
                    // if(buf.capacity() == 0)
                    // {
                    //     LOG.info("key: " + key.toString());
                    // }
                    key.verify(value);
                } else {
                    ByteBuffer value = rows.get(0).getBytes(1);
                    byte[] bytes = new byte[value.capacity()];
                    value.get(bytes);
                    verifyRandomValue(key, bytes);
                }
            //}
            LOG.debug("Read key: " + key.toString());
            return 1;
    
    }



  @Override
  public long doWrite(int threadIdx) {
    SimpleLoadGenerator.Key key = getSimpleLoadGenerator().getKeyToWrite();
    if (key == null) {
      return 0;
    }

    try {
        //synchronized (unpreparedInitLock) {
        // Do the write to Cassandra.
        //String insert;
        String insert;
        if (appConfig.valueSize == 0) {
            String value = key.getValueStr();
            // LOG.fatal("value: "+ value);
            //if (unpreparedInsert == null) {
               insert = queryInsert(key.asString(), ByteBuffer.wrap(value.getBytes()));
            //}
        } else {
            byte[] value = getRandomValue(key);
            ////String strvalue = new String(value, charset);
            // LOG.fatal("key: "+ key.asString());
            //if (unpreparedInsert == null) {
                insert = queryInsert(key.asString(), ByteBuffer.wrap(value));
            //}
        }
        ResultSet resultSet = getCassandraClient().execute(insert);
        LOG.debug("Wrote key: " + key.toString() + ", return code: " + resultSet.toString());
      //  LOG.info("Wrote key: " + key.toString());
        getSimpleLoadGenerator().recordWriteSuccess(key);
        return 1;
    //}
    } catch (Exception e) {
        getSimpleLoadGenerator().recordWriteFailure(key);
        throw e;
    }
  }

  @Override
  public void appendMessage(StringBuilder sb) {
    super.appendMessage(sb);
    sb.append("maxWrittenKey: " + getSimpleLoadGenerator().getMaxWrittenKey() +  " | ");
    sb.append("maxGeneratedKey: " + getSimpleLoadGenerator().getMaxGeneratedKey() +  " | ");
  }

  public void appendParentMessage(StringBuilder sb) {
    super.appendMessage(sb);
  }

  @Override
  public List<String> getWorkloadDescription() {
    return Arrays.asList(
        "Sample key-value app built on Cassandra. The app writes out 1M unique string keys",
        "each with a string value. There are multiple readers and writers that update these",
        "keys and read them indefinitely. Note that the number of reads and writes to",
        "perform can be specified as a parameter.");
  }

  @Override
  public List<String> getWorkloadOptionalArguments() {
    return Arrays.asList(
        "--num_unique_keys " + appConfig.numUniqueKeysToWrite,
        "--num_reads " + appConfig.numKeysToRead,
        "--num_writes " + appConfig.numKeysToWrite,
        "--value_size " + appConfig.valueSize,
        "--num_threads_read " + appConfig.numReaderThreads,
        "--num_threads_write " + appConfig.numWriterThreads,
        "--table_ttl_seconds " + appConfig.tableTTLSeconds);
  }
}
