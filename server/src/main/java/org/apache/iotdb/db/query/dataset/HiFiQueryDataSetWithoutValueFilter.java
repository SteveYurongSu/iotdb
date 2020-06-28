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
import org.apache.iotdb.tsfile.utils.PublicBAOS;
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

  private TreeSet<Long> timeHeap;

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

  // capacity for blocking queue
  private static final int BLOCKING_QUEUE_CAPACITY = 5;

  private static final QueryTaskPoolManager TASK_POOL_MANAGER = QueryTaskPoolManager.getInstance();

  private static final Logger LOGGER = LoggerFactory
      .getLogger(RawQueryDataSetWithoutValueFilter.class);

  public HiFiQueryDataSetWithoutValueFilter(HiFiQueryPlan queryPlan,
      List<ManagedSeriesReader> readers) throws IOException, InterruptedException {
    super(queryPlan.getDeduplicatedPaths(), queryPlan.getDeduplicatedDataTypes());
    this.queryPlan = queryPlan;
    weightOperators = new WeightOperator[readers.size()];
    for (int i = 0; i < readers.size(); ++i) {
      weightOperators[i] = WeightOperator.getWeightOperator(queryPlan.getHiFiWeightOperatorName(),
          getDataTypes().get(i));
    }
    sampleOperators = new SampleOperator[readers.size()];
    for (int i = 0; i < readers.size(); ++i) {
      sampleOperators[i] = SampleOperator.getSampleOperator(queryPlan.getHiFiSampleOperatorName(),
          getDataTypes().get(i));
    }
    bucketWeights = new double[readers.size()];
    seriesReaderList = readers;
    blockingQueueArray = new BlockingQueue[readers.size()];
    for (int i = 0; i < seriesReaderList.size(); i++) {
      blockingQueueArray[i] = new LinkedBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
    }
    cachedBatchDataArray = new BatchData[readers.size()];
    noMoreDataInQueueArray = new boolean[readers.size()];
    init();
  }

  private void init() throws IOException, InterruptedException {
    timeHeap = new TreeSet<>();
    for (int i = 0; i < seriesReaderList.size(); i++) {
      ManagedSeriesReader reader = seriesReaderList.get(i);
      reader.setHasRemaining(true);
      reader.setManagedByQueryManager(true);
      TASK_POOL_MANAGER
          .submit(new ReadTask(reader, blockingQueueArray[i], paths.get(i).getFullPath()));
    }
    for (int i = 0; i < seriesReaderList.size(); i++) {
      fillCache(i);
      // try to put the next timestamp into the heap
      if (cachedBatchDataArray[i] != null && cachedBatchDataArray[i].hasCurrent()) {
        long time = cachedBatchDataArray[i].currentTime();
        timeHeap.add(time);
      }
    }
  }

  /**
   * for RPC in RawData query between client and server fill time buffer, value buffers and bitmap
   * buffers
   */
  public TSQueryDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder)
      throws IOException, InterruptedException { // todo: encoder, rowLimit, fetchSize ...
    int seriesNum = seriesReaderList.size();
    List<Long> originalTimestamps = new ArrayList<>();
    List[] originalValuesList = new List[seriesNum];
    List<Byte>[] originalBitmapList = new List[seriesNum];
    List<Double>[] originalWeightsList = new List[seriesNum];
    PublicBAOS timeBAOS = new PublicBAOS();
    PublicBAOS[] valueBAOSList = new PublicBAOS[seriesNum];
    PublicBAOS[] bitmapBAOSList = new PublicBAOS[seriesNum];
    for (int seriesIndex = 0; seriesIndex < seriesNum; ++seriesIndex) {
      originalValuesList[seriesIndex] = new ArrayList<>();
      originalBitmapList[seriesIndex] = new ArrayList<>();
      originalWeightsList[seriesIndex] = new ArrayList<>();
      valueBAOSList[seriesIndex] = new PublicBAOS();
      bitmapBAOSList[seriesIndex] = new PublicBAOS();
    }

    fetch(originalTimestamps, originalValuesList, originalBitmapList);
    calculatePointWeightsAndBucketWeights(originalTimestamps, originalValuesList,
        originalBitmapList, originalWeightsList);
    hiFiSample(originalTimestamps, originalValuesList, originalBitmapList, originalWeightsList,
        timeBAOS, valueBAOSList, bitmapBAOSList);

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

  private void fetch(List<Long> originalTimestamps, List<Number>[] originalValuesList,
      List<Byte>[] originalBitmapList) throws IOException, InterruptedException {
    int seriesNum = seriesReaderList.size();

    while (!timeHeap.isEmpty()) {
      Long minTime = timeHeap.pollFirst();
      originalTimestamps.add(minTime);

      for (int seriesIndex = 0; seriesIndex < seriesNum; ++seriesIndex) {
        if (cachedBatchDataArray[seriesIndex] == null
            || !cachedBatchDataArray[seriesIndex].hasCurrent()
            || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
          // current batch is empty or does not have value at minTime
          originalBitmapList[seriesIndex].add((byte) 0B0);
        } else {
          // current batch has value at minTime, consume current value
          originalBitmapList[seriesIndex].add((byte) 0B1);
          TSDataType type = cachedBatchDataArray[seriesIndex].getDataType();
          switch (type) {
            case INT32:
              originalValuesList[seriesIndex].add(cachedBatchDataArray[seriesIndex].getInt());
              break;
            case INT64:
              originalValuesList[seriesIndex].add(cachedBatchDataArray[seriesIndex].getLong());
              break;
            case FLOAT:
              originalValuesList[seriesIndex].add(cachedBatchDataArray[seriesIndex].getFloat());
              break;
            case DOUBLE:
              originalValuesList[seriesIndex].add(cachedBatchDataArray[seriesIndex].getDouble());
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
          // try to put the next timestamp into the heap
          if (cachedBatchDataArray[seriesIndex].hasCurrent()) {
            timeHeap.add(cachedBatchDataArray[seriesIndex].currentTime());
          }
        }
      }
    }
  }

  private void calculatePointWeightsAndBucketWeights(List<Long> originalTimestamps,
      List[] originalValuesList, List<Byte>[] originalBitmapList,
      List<Double>[] originalWeightsList) {
    int seriesNum = seriesReaderList.size();
    for (int seriesIndex = 0; seriesIndex < seriesNum; ++seriesIndex) {
      weightOperators[seriesIndex].calculate(originalTimestamps, originalValuesList[seriesIndex],
          originalBitmapList[seriesIndex], originalWeightsList[seriesIndex]);
      double bucketSize = queryPlan.getAverageBucketSize()[seriesIndex];
      bucketWeights[seriesIndex] =
          bucketSize <= 1 ? 0 : bucketSize * weightOperators[seriesIndex].getCurrentAverageWeight();
    }
  }

  private void hiFiSample(List<Long> originalTimestamps, List[] originalValuesList,
      List<Byte>[] originalBitmapList, List<Double>[] originalWeightsList, PublicBAOS timeBAOS,
      PublicBAOS[] valueBAOSList, PublicBAOS[] bitmapBAOSList) {
    int seriesNum = seriesReaderList.size();
    for (int seriesIndex = 0; seriesIndex < seriesNum; ++seriesIndex) {
      sampleOperators[seriesIndex].sample(originalTimestamps, originalValuesList[seriesIndex],
          originalBitmapList[seriesIndex], originalWeightsList[seriesIndex], timeBAOS,
          valueBAOSList[seriesIndex], bitmapBAOSList[seriesIndex], getDataTypes().get(seriesIndex),
          bucketWeights[seriesIndex]);
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
  protected RowRecord nextWithoutConstraint() throws IOException {
    int seriesNum = seriesReaderList.size();

    long minTime = timeHeap.pollFirst();

    RowRecord record = new RowRecord(minTime);

    for (int seriesIndex = 0; seriesIndex < seriesNum; ++seriesIndex) {
      if (cachedBatchDataArray[seriesIndex] == null
          || !cachedBatchDataArray[seriesIndex].hasCurrent()
          || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
        record.addField(null);
      } else {
        TSDataType dataType = dataTypes.get(seriesIndex);
        record.addField(cachedBatchDataArray[seriesIndex].currentValue(), dataType);

        // move next
        cachedBatchDataArray[seriesIndex].next();

        // get next batch if current batch is empty and still have remaining batch data in queue
        if (!cachedBatchDataArray[seriesIndex].hasCurrent()
            && !noMoreDataInQueueArray[seriesIndex]) {
          try {
            fillCache(seriesIndex);
          } catch (InterruptedException e) {
            LOGGER.error("Interrupted while taking from the blocking queue: ", e);
            Thread.currentThread().interrupt();
          } catch (IOException e) {
            LOGGER.error("Got IOException", e);
            throw e;
          }
        }

        // try to put the next timestamp into the heap
        if (cachedBatchDataArray[seriesIndex].hasCurrent()) {
          timeHeap.add(cachedBatchDataArray[seriesIndex].currentTime());
        }
      }
    }

    return record;
  }
}
