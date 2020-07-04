/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.dataset;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.qp.physical.crud.HiFiQueryPlan;
import org.apache.iotdb.db.query.hifi.sample.SampleOperator;
import org.apache.iotdb.db.query.hifi.weight.WeightOperator;
import org.apache.iotdb.db.query.pool.QueryTaskPoolManager;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.ExceptionBatchData;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.SignalBatchData;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiFiQueryDataSetWithoutValueFilter extends QueryDataSet {

  private static class ReadTask extends WrappedRunnable {

    private final ManagedSeriesReader reader;
    private final String pathName;
    private final BlockingQueue<BatchData> blockingQueue;

    public ReadTask(ManagedSeriesReader reader, BlockingQueue<BatchData> blockingQueue,
        String pathName) {
      this.reader = reader;
      this.blockingQueue = blockingQueue;
      this.pathName = pathName;
    }

    @Override
    public void runMayThrow() {
      try {
        synchronized (reader) {
          // if the task is submitted, there must be free space in the queue
          // so here we don't need to check whether the queue has free space
          // the reader has next batch
          while (reader.hasNextBatch()) {
            BatchData batchData = reader.nextBatch();
            // iterate until we get first batch data with valid value
            if (batchData.isEmpty()) {
              continue;
            }
            blockingQueue.put(batchData);
            // if the queue also has free space, just submit another itself
            if (blockingQueue.remainingCapacity() > 0) {
              TASK_POOL_MANAGER.submit(this);
            }
            // the queue has no more space
            // remove itself from the QueryTaskPoolManager
            else {
              reader.setManagedByQueryManager(false);
            }
            return;
          }
          // there are no batch data left in this reader
          // put the signal batch data into queue
          blockingQueue.put(SignalBatchData.getInstance());
          // set the hasRemaining field in reader to false
          // tell the Consumer not to submit another task for this reader any more
          reader.setHasRemaining(false);
          // remove itself from the QueryTaskPoolManager
          reader.setManagedByQueryManager(false);
        }
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted while putting into the blocking queue: ", e);
        Thread.currentThread().interrupt();
        reader.setHasRemaining(false);
      } catch (IOException e) {
        putExceptionBatchData(e, String
            .format("Something gets wrong while reading from the series reader %s: ", pathName));
      } catch (Exception e) {
        putExceptionBatchData(e, "Something gets wrong: ");
      }
    }

    private void putExceptionBatchData(Exception e, String logMessage) {
      try {
        LOGGER.error(logMessage, e);
        reader.setHasRemaining(false);
        blockingQueue.put(new ExceptionBatchData(e));
      } catch (InterruptedException ex) {
        LOGGER.error("Interrupted while putting ExceptionBatchData into the blocking queue: ", ex);
        Thread.currentThread().interrupt();
      }
    }
  }

  private final HiFiQueryPlan queryPlan;

  private final List<ManagedSeriesReader> seriesReaderList;

  // Blocking queue list for each batch reader
  private final BlockingQueue<BatchData>[] blockingQueueArray;

  // indicate that there is no more batch data in the corresponding queue
  // in case that the consumer thread is blocked on the queue and won't get runnable any more
  // this field is not same as the `hasRemaining` in SeriesReaderWithoutValueFilter
  // even though the `hasRemaining` in SeriesReaderWithoutValueFilter is false
  // noMoreDataInQueue can still be true
  // its usage is to tell the consumer thread not to call the take() method.
  private final boolean[] noMoreDataInQueueArray;

  private final BatchData[] cachedBatchDataArray;

  private final WeightOperator<?>[] weightOperators;
  private final SampleOperator<?>[] sampleOperators;
  private final double[] bucketWeights;

  private final List<Long>[] originalTimestampsList;
  private final List<Number>[] originalValuesList;
  private final List<Double>[] originalWeightsList;

  private final List<Long>[] sampledTimestampsList;
  private final List<Number>[] sampledValuesList;

  private final int[] readyToConsumeIndexList;
  private final TreeSet<Long> timeHeap;

  private static final int FLAG = 0x01;

  // capacity for blocking queue
  private static final int BLOCKING_QUEUE_CAPACITY = 5;

  private static final QueryTaskPoolManager TASK_POOL_MANAGER = QueryTaskPoolManager.getInstance();

  private static final Logger LOGGER = LoggerFactory
      .getLogger(RawQueryDataSetWithoutValueFilter.class);

  public HiFiQueryDataSetWithoutValueFilter(HiFiQueryPlan queryPlan,
      List<ManagedSeriesReader> readers) throws IOException, InterruptedException {
    super(queryPlan.getDeduplicatedPaths(), queryPlan.getDeduplicatedDataTypes());
    this.queryPlan = queryPlan;
    seriesReaderList = readers;
    int seriesNum = readers.size();
    blockingQueueArray = new BlockingQueue[seriesNum];
    noMoreDataInQueueArray = new boolean[seriesNum];
    cachedBatchDataArray = new BatchData[seriesNum];
    weightOperators = new WeightOperator[seriesNum];
    sampleOperators = new SampleOperator[seriesNum];
    bucketWeights = new double[seriesNum];
    originalTimestampsList = new List[seriesNum];
    originalValuesList = new List[seriesNum];
    originalWeightsList = new List[seriesNum];
    sampledTimestampsList = new List[seriesNum];
    sampledValuesList = new List[seriesNum];
    for (int i = 0; i < seriesNum; ++i) {
      blockingQueueArray[i] = new LinkedBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
      weightOperators[i] = WeightOperator.getWeightOperator(queryPlan.getHiFiWeightOperatorName(),
          getDataTypes().get(i));
      sampleOperators[i] = SampleOperator.getSampleOperator(queryPlan.getHiFiSampleOperatorName(),
          getDataTypes().get(i));
      originalTimestampsList[i] = new ArrayList<>();
      originalValuesList[i] = new ArrayList<>();
      originalWeightsList[i] = new ArrayList<>();
      sampledTimestampsList[i] = new ArrayList<>();
      sampledValuesList[i] = new ArrayList<>();
    }
    init();
    fetch();
    calculatePointWeights();
    calculateBucketWeights();
    hiFiSample();
    readyToConsumeIndexList = new int[seriesNum];
    timeHeap = new TreeSet<>();
    for (List<Long> sampledTimestamps : sampledTimestampsList) {
      timeHeap.addAll(sampledTimestamps);
    }
  }

  private void init() throws IOException, InterruptedException {
    for (int i = 0; i < seriesReaderList.size(); ++i) {
      ManagedSeriesReader reader = seriesReaderList.get(i);
      reader.setHasRemaining(true);
      reader.setManagedByQueryManager(true);
      TASK_POOL_MANAGER
          .submit(new ReadTask(reader, blockingQueueArray[i], paths.get(i).getFullPath()));
    }
    for (int i = 0; i < seriesReaderList.size(); ++i) {
      fillCache(i);
    }
  }

  /**
   * for RPC in RawData query between client and server fill time buffer, value buffers and bitmap
   * buffers
   */
  public TSQueryDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder) throws IOException {
    int seriesNum = seriesReaderList.size();
    PublicBAOS timeBAOS = new PublicBAOS();
    PublicBAOS[] valueBAOSList = new PublicBAOS[seriesNum];
    PublicBAOS[] bitmapBAOSList = new PublicBAOS[seriesNum];
    for (int seriesIndex = 0; seriesIndex < seriesNum; ++seriesIndex) {
      valueBAOSList[seriesIndex] = new PublicBAOS();
      bitmapBAOSList[seriesIndex] = new PublicBAOS();
    }

    fillBAOS(fetchSize, encoder, timeBAOS, valueBAOSList, bitmapBAOSList);

    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();
    // set time buffer
    ByteBuffer timeBuffer = ByteBuffer.allocate(timeBAOS.size());
    timeBuffer.put(timeBAOS.getBuf(), 0, timeBAOS.size());
    timeBuffer.flip();
    tsQueryDataSet.setTime(timeBuffer);
    // set value buffers and bitmap buffers
    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();
    for (int seriesIndex = 0; seriesIndex < seriesNum; ++seriesIndex) {
      // add value buffer of current series
      putPBOSToBuffer(valueBAOSList, valueBufferList, seriesIndex);
      // add bitmap buffer of current series
      putPBOSToBuffer(bitmapBAOSList, bitmapBufferList, seriesIndex);
    }
    tsQueryDataSet.setValueList(valueBufferList);
    tsQueryDataSet.setBitmapList(bitmapBufferList);
    return tsQueryDataSet;
  }

  private void fetch() throws IOException, InterruptedException {
    int seriesNum = seriesReaderList.size();
    for (int seriesIndex = 0; seriesIndex < seriesNum; ++seriesIndex) {
      while (cachedBatchDataArray[seriesIndex] != null
          && cachedBatchDataArray[seriesIndex].hasCurrent()) {
        originalTimestampsList[seriesIndex]
            .add(cachedBatchDataArray[seriesIndex].currentWrappedTime());
        TSDataType type = cachedBatchDataArray[seriesIndex].getDataType();
        switch (type) {
          case INT32:
            originalValuesList[seriesIndex].add(cachedBatchDataArray[seriesIndex].getWrappedInt());
            break;
          case INT64:
            originalValuesList[seriesIndex].add(cachedBatchDataArray[seriesIndex].getWrappedLong());
            break;
          case FLOAT:
            originalValuesList[seriesIndex]
                .add(cachedBatchDataArray[seriesIndex].getWrappedFloat());
            break;
          case DOUBLE:
            originalValuesList[seriesIndex]
                .add(cachedBatchDataArray[seriesIndex].getWrappedDouble());
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }

        // move next
        cachedBatchDataArray[seriesIndex].next();
        // get next batch if current batch is empty and still have remaining batch data in queue
        if (!cachedBatchDataArray[seriesIndex].hasCurrent()
            && !noMoreDataInQueueArray[seriesIndex]) {
          fillCache(seriesIndex);
        }
      }
    }
  }

  private void calculatePointWeights() {
    int seriesNum = seriesReaderList.size();
    for (int seriesIndex = 0; seriesIndex < seriesNum; ++seriesIndex) {
      weightOperators[seriesIndex]
          .calculate(originalTimestampsList[seriesIndex], (List) originalValuesList[seriesIndex],
              originalWeightsList[seriesIndex]);
    }
  }

  private void calculateBucketWeights() {
    int seriesNum = seriesReaderList.size();
    for (int seriesIndex = 0; seriesIndex < seriesNum; ++seriesIndex) {
      double bucketSize =
          (double) originalTimestampsList[seriesIndex].size() / queryPlan.getHiFiSampleSize();
      bucketWeights[seriesIndex] =
          bucketSize <= 1 ? 0 : bucketSize * weightOperators[seriesIndex].getCurrentAverageWeight();
    }
  }

  private void hiFiSample() {
    int seriesNum = seriesReaderList.size();
    for (int seriesIndex = 0; seriesIndex < seriesNum; ++seriesIndex) {
      sampleOperators[seriesIndex].sample(originalTimestampsList[seriesIndex],
          (List) originalValuesList[seriesIndex], originalWeightsList[seriesIndex],
          sampledTimestampsList[seriesIndex], (List) sampledValuesList[seriesIndex],
          bucketWeights[seriesIndex]);
    }
  }

  private void fillBAOS(int fetchSize, WatermarkEncoder encoder, PublicBAOS timeBAOS,
      PublicBAOS[] valueBAOSList, PublicBAOS[] bitmapBAOSList) throws IOException {
    int seriesNum = seriesReaderList.size();
    int[] currentBitmapList = new int[seriesNum]; // used to record a bitmap for every 8 row records
    int rowCount = 0;

    while (rowCount < fetchSize) {
      if ((rowLimit > 0 && alreadyReturnedRowNum >= rowLimit) || timeHeap.isEmpty()) {
        break;
      }

      long minTime = timeHeap.pollFirst();
      if (rowOffset == 0) {
        timeBAOS.write(BytesUtils.longToBytes(minTime));
      }

      for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
        int currentIndex = readyToConsumeIndexList[seriesIndex];
        if (sampledTimestampsList[seriesIndex].size() <= currentIndex
            || sampledTimestampsList[seriesIndex].get(currentIndex) != minTime) {
          // current series does not have value at minTime
          if (rowOffset == 0) {
            currentBitmapList[seriesIndex] = (currentBitmapList[seriesIndex] << 1);
          }
        } else {
          // current series has value at minTime, write the value info BAOS
          if (rowOffset == 0) {
            currentBitmapList[seriesIndex] = (currentBitmapList[seriesIndex] << 1) | FLAG;
            TSDataType type = getDataTypes().get(seriesIndex);
            switch (type) {
              case INT32:
                int intValue = sampledValuesList[seriesIndex].get(currentIndex).intValue();
                if (encoder != null && encoder.needEncode(minTime)) {
                  intValue = encoder.encodeInt(intValue, minTime);
                }
                ReadWriteIOUtils.write(intValue, valueBAOSList[seriesIndex]);
                break;
              case INT64:
                long longValue = sampledValuesList[seriesIndex].get(currentIndex).longValue();
                if (encoder != null && encoder.needEncode(minTime)) {
                  longValue = encoder.encodeLong(longValue, minTime);
                }
                ReadWriteIOUtils.write(longValue, valueBAOSList[seriesIndex]);
                break;
              case FLOAT:
                float floatValue = sampledValuesList[seriesIndex].get(currentIndex).floatValue();
                if (encoder != null && encoder.needEncode(minTime)) {
                  floatValue = encoder.encodeFloat(floatValue, minTime);
                }
                ReadWriteIOUtils.write(floatValue, valueBAOSList[seriesIndex]);
                break;
              case DOUBLE:
                double doubleValue = sampledValuesList[seriesIndex].get(currentIndex).doubleValue();
                if (encoder != null && encoder.needEncode(minTime)) {
                  doubleValue = encoder.encodeDouble(doubleValue, minTime);
                }
                ReadWriteIOUtils.write(doubleValue, valueBAOSList[seriesIndex]);
                break;
              default:
                throw new UnSupportedDataTypeException(
                    String.format("Data type %s is not supported.", type));
            }
          }
          // update index
          ++readyToConsumeIndexList[seriesIndex];
        }
      }

      if (rowOffset == 0) {
        rowCount++;
        if (rowCount % 8 == 0) {
          for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
            ReadWriteIOUtils
                .write((byte) currentBitmapList[seriesIndex], bitmapBAOSList[seriesIndex]);
            // we should clear the bitmap every 8 row record
            currentBitmapList[seriesIndex] = 0;
          }
        }
        if (rowLimit > 0) {
          alreadyReturnedRowNum++;
        }
      } else {
        rowOffset--;
      }
    }

    /*
     * feed the bitmap with remaining 0 in the right
     * if current bitmap is 00011111 and remaining is 3, after feeding the bitmap is 11111000
     */
    if (rowCount > 0) {
      int remaining = rowCount % 8;
      if (remaining != 0) {
        for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
          ReadWriteIOUtils.write((byte) (currentBitmapList[seriesIndex] << (8 - remaining)),
              bitmapBAOSList[seriesIndex]);
        }
      }
    }
  }

  private void fillCache(int seriesIndex) throws IOException, InterruptedException {
    BatchData batchData = blockingQueueArray[seriesIndex].take();
    // no more batch data in this time series queue
    if (batchData instanceof SignalBatchData) {
      noMoreDataInQueueArray[seriesIndex] = true;
    } else if (batchData instanceof ExceptionBatchData) {
      // exception happened in producer thread
      ExceptionBatchData exceptionBatchData = (ExceptionBatchData) batchData;
      LOGGER.error("exception happened in producer thread", exceptionBatchData.getException());
      if (exceptionBatchData.getException() instanceof IOException) {
        throw (IOException) exceptionBatchData.getException();
      } else if (exceptionBatchData.getException() instanceof RuntimeException) {
        throw (RuntimeException) exceptionBatchData.getException();
      }
    } else {   // there are more batch data in this time series queue
      cachedBatchDataArray[seriesIndex] = batchData;
      synchronized (seriesReaderList.get(seriesIndex)) {
        // we only need to judge whether to submit another task when the queue is not full
        if (blockingQueueArray[seriesIndex].remainingCapacity() > 0) {
          ManagedSeriesReader reader = seriesReaderList.get(seriesIndex);
          // if the reader isn't being managed and still has more data,
          // that means this read task leave the pool before because the queue has no more space
          // now we should submit it again
          if (!reader.isManagedByQueryManager() && reader.hasRemaining()) {
            reader.setManagedByQueryManager(true);
            TASK_POOL_MANAGER.submit(new ReadTask(reader, blockingQueueArray[seriesIndex],
                paths.get(seriesIndex).getFullPath()));
          }
        }
      }
    }
  }

  private void putPBOSToBuffer(PublicBAOS[] bitmapBAOSList, List<ByteBuffer> bitmapBufferList,
      int tsIndex) {
    ByteBuffer bitmapBuffer = ByteBuffer.allocate(bitmapBAOSList[tsIndex].size());
    bitmapBuffer.put(bitmapBAOSList[tsIndex].getBuf(), 0, bitmapBAOSList[tsIndex].size());
    bitmapBuffer.flip();
    bitmapBufferList.add(bitmapBuffer);
  }

  /**
   * for spark/hadoop/hive integration and test
   */
  @Override
  protected boolean hasNextWithoutConstraint() {
    return !timeHeap.isEmpty();
  }

  /**
   * for spark/hadoop/hive integration and test
   */
  @Override
  protected RowRecord nextWithoutConstraint() {
    int seriesNum = seriesReaderList.size();
    long minTime = timeHeap.pollFirst();
    RowRecord record = new RowRecord(minTime);

    for (int seriesIndex = 0; seriesIndex < seriesNum; ++seriesIndex) {
      int currentIndex = readyToConsumeIndexList[seriesIndex];
      if (sampledTimestampsList[seriesIndex].size() <= currentIndex
          || sampledTimestampsList[seriesIndex].get(currentIndex) != minTime) {
        record.addField(null);
      } else {
        record.addField(sampledValuesList[seriesIndex].get(currentIndex),
            getDataTypes().get(seriesIndex));
        // update index
        ++readyToConsumeIndexList[seriesIndex];
      }
    }

    return record;
  }
}
