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
package org.apache.iotdb.tsfile;

import fi.iki.yak.ts.compression.gorilla.BitOutput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;
import fi.iki.yak.ts.compression.gorilla.GorillaCompressor;
import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import fi.iki.yak.ts.compression.gorilla.Pair;
import fi.iki.yak.ts.compression.gorilla.Predictor;
import fi.iki.yak.ts.compression.gorilla.ValueCompressor;
import fi.iki.yak.ts.compression.gorilla.ValueDecompressor;
import fi.iki.yak.ts.compression.gorilla.predictors.LastValuePredictor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder.LongDeltaDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.DoublePrecisionDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder.LongDeltaEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DoublePrecisionEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class TsFileSequenceRead {

  private static int count = 0;
  private static int bytesLengthBeforeEncoding = 0;

  private static int t1v1 = 0;
  private static int v1 = 0;
  private static int t1 = 0;
  private static int v2 = 0;
  private static int t2 = 0;

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void main(String[] args) throws IOException {
    String filename = "/Users/steve/Desktop/IoTDB/1599784379866-741-0.tsfile-0-1600049975086.vm";
    if (args.length >= 1) {
      filename = args[0];
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
      System.out
          .println("file length: " + FSFactoryProducer.getFSFactory().getFile(filename).length());
      System.out.println("file magic head: " + reader.readHeadMagic());
      System.out.println("file magic tail: " + reader.readTailMagic());
      System.out.println("Level 1 metadata position: " + reader.getFileMetadataPos());
      System.out.println("Level 1 metadata size: " + reader.getFileMetadataSize());
      // Sequential reading of one ChunkGroup now follows this order:
      // first SeriesChunks (headers and data) in one ChunkGroup, then the CHUNK_GROUP_FOOTER
      // Because we do not know how many chunks a ChunkGroup may have, we should read one byte (the marker) ahead and
      // judge accordingly.
      reader
          .position((long) TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.VERSION_NUMBER
              .getBytes().length);
      System.out.println("[Chunk Group]");
      System.out.println("position: " + reader.position());
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            System.out.println("\t[Chunk]");
            System.out.println("\tposition: " + reader.position());
            ChunkHeader header = reader.readChunkHeader();
            System.out.println("\tMeasurement: " + header.getMeasurementID());
            Decoder defaultTimeDecoder = Decoder.getDecoderByType(
                TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                TSDataType.INT64);
            Decoder valueDecoder = Decoder
                .getDecoderByType(header.getEncodingType(), header.getDataType());
            for (int j = 0; j < header.getNumOfPages(); j++) {
              valueDecoder.reset();
              System.out.println("\t\t[Page]\n \t\tPage head position: " + reader.position());
              PageHeader pageHeader = reader.readPageHeader(header.getDataType());
              System.out.println("\t\tPage data position: " + reader.position());
              System.out.println("\t\tpoints in the page: " + pageHeader.getNumOfValues());
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              System.out
                  .println("\t\tUncompressed page data size: " + pageHeader.getUncompressedSize());
              System.out
                  .println("\t\tCompressed page data size: " + pageHeader.getCompressedSize());
              PageReader reader1 = new PageReader(pageData, header.getDataType(), valueDecoder,
                  defaultTimeDecoder, null);
              BatchData batchData = reader1.getAllSatisfiedPageData();
              if (header.getDataType().equals(TSDataType.DOUBLE)) {
                System.out.println("------------------------------------------------");
                System.out.printf("第%d个header：%n\n", count + 1);
                calculateCompressionRatio(batchData);
                if (10 < ++count) {
                  return;
                }
              } else {
                throw new RuntimeException();
              }
            }
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            System.out.println("Chunk Group Footer position: " + reader.position());
            ChunkGroupFooter chunkGroupFooter = reader.readChunkGroupFooter();
            System.out.println("device: " + chunkGroupFooter.getDeviceID());
            break;
          case MetaMarker.VERSION:
            long version = reader.readVersion();
            System.out.println("version: " + version);
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      System.out.println("[Metadata]");
      for (String device : reader.getAllDevices()) {
        Map<String, List<ChunkMetadata>> seriesMetaData = reader.readChunkMetadataInDevice(device);
        System.out.println(String
            .format("\t[Device]Device %s, Number of Measurements %d", device,
                seriesMetaData.size()));
        for (Map.Entry<String, List<ChunkMetadata>> serie : seriesMetaData.entrySet()) {
          System.out.println("\t\tMeasurement:" + serie.getKey());
          for (ChunkMetadata chunkMetadata : serie.getValue()) {
            System.out.println("\t\tFile offset:" + chunkMetadata.getOffsetOfChunkHeader());
          }
        }
      }
    }
  }

  private static void calculateCompressionRatio(BatchData batchData) throws IOException {
    calculateByteLengthBeforeEncoding(batchData);
    batchData.resetBatchData();
    pre(batchData);
    batchData.resetBatchData();
    t1v1(batchData);
    batchData.resetBatchData();
    t2(batchData);
    batchData.resetBatchData();
    v2(batchData);
    System.out.printf("T1 + V1 after encoding: %d, %f\n", t1 + v1,
        (t1 + v1) / (double) (2 * bytesLengthBeforeEncoding));
    System.out.printf("T1 + V2 after encoding: %d, %f\n", t1 + v2,
        (t1 + v2) / (double) (2 * bytesLengthBeforeEncoding));
    System.out.printf("T2 + V1 after encoding: %d, %f\n", t2 + v1,
        (t2 + v1) / (double) (2 * bytesLengthBeforeEncoding));
    System.out.printf("T2 + V2 after encoding: %d, %f\n", t2 + v2,
        (t2 + v2) / (double) (2 * bytesLengthBeforeEncoding));
  }

  private static void calculateByteLengthBeforeEncoding(BatchData batchData) {
    int count = 0;
    while (batchData.hasCurrent()) {
      ++count;
      batchData.next();
    }
    bytesLengthBeforeEncoding = 8 * count;
    System.out.printf("bytes length: %d\n\n", bytesLengthBeforeEncoding);
  }

  private static void pre(BatchData batchData) throws IOException {
    ByteBufferBitOutput compressedOutput = new ByteBufferBitOutput();
    PublicBAOS uncompressedOutput = new PublicBAOS();

    GorillaCompressor compressor = new GorillaCompressor(batchData.currentTime(), compressedOutput,
        new LastValuePredictor());
    while (batchData.hasCurrent()) {
      compressor.addValue(batchData.currentTime(), batchData.getDouble());
      ReadWriteIOUtils.write(batchData.currentTime(), uncompressedOutput);
      ReadWriteIOUtils.write(batchData.getDouble(), uncompressedOutput);
      batchData.next();
    }
    compressor.close();

    batchData.resetBatchData();
    compressedOutput.getByteBuffer().flip();
    GorillaDecompressor decompressor = new GorillaDecompressor(
        new ByteBufferBitInput(compressedOutput.getByteBuffer()), new LastValuePredictor());
    while (batchData.hasCurrent()) {
      Pair pair = decompressor.readPair();
      if (pair.getTimestamp() != batchData.currentTime()
          || pair.getDoubleValue() != batchData.getDouble()) {
        throw new RuntimeException();
      }
      batchData.next();
    }

    if (uncompressedOutput.size() != bytesLengthBeforeEncoding * 2) {
      throw new RuntimeException();
    }

    t1v1 = compressedOutput.getByteBuffer().position();
    System.out.printf("T1 + V1 after encoding: %d\n", t1v1);
  }

  private static void t1v1(BatchData batchData) throws IOException {
    ByteBufferBitOutput compressedOutput = new ByteBufferBitOutput();
    PublicBAOS uncompressedOutput = new PublicBAOS();

    Predictor predictor = new LastValuePredictor();
    double first = batchData.getDouble();
    predictor.update(Double.doubleToRawLongBits(first));
    compressedOutput.writeBits(Double.doubleToRawLongBits(first), 64);
    ReadWriteIOUtils.write(first, uncompressedOutput);
    batchData.next();

    PublicValueCompressor compressor = new PublicValueCompressor(compressedOutput, predictor);
    while (batchData.hasCurrent()) {
      compressor.add(batchData.getDouble());
      ReadWriteIOUtils.write(batchData.getDouble(), uncompressedOutput);
      batchData.next();
    }
    compressor.close();

    batchData.resetBatchData();
    compressedOutput.getByteBuffer().flip();
    ValueDecompressor decompressor = new ValueDecompressor(
        new ByteBufferBitInput(compressedOutput.getByteBuffer()), new LastValuePredictor());
    if (batchData.getDouble() != Double.longBitsToDouble(decompressor.readFirst())) {
      throw new RuntimeException();
    }
    batchData.next();
    while (batchData.hasCurrent()) {
      if (batchData.getDouble() != Double.longBitsToDouble(decompressor.nextValue())) {
        throw new RuntimeException();
      }
      batchData.next();
    }

    if (uncompressedOutput.size() != bytesLengthBeforeEncoding) {
      throw new RuntimeException();
    }

    v1 = compressedOutput.getByteBuffer().position();
    t1 = t1v1 - v1;
    System.out.printf("T1 after encoding: %d, %f\n", t1, t1 / (double) bytesLengthBeforeEncoding);
    System.out.printf("V1 after encoding: %d, %f\n", v1, v1 / (double) bytesLengthBeforeEncoding);
  }

  private static void t2(BatchData batchData) throws IOException {
    PublicBAOS baos = new PublicBAOS();
    Encoder encoder = new LongDeltaEncoder();

    while (batchData.hasCurrent()) {
      encoder.encode(batchData.currentTime(), baos);
      batchData.next();
    }
    encoder.flush(baos);

    t2 = baos.size();
    System.out.printf("T2 after encoding: %d, %f\n", t2, t2 / (double) bytesLengthBeforeEncoding);

    ByteBuffer byteBuffer = ByteBuffer.wrap(baos.getBuf(), 0, baos.size());
    byteBuffer.flip();
    batchData.resetBatchData();
    LongDeltaDecoder decoder = new LongDeltaDecoder();
    while (decoder.hasNext(byteBuffer)) {
      if (decoder.readLong(byteBuffer) != batchData.currentTime()) {
        throw new RuntimeException();
      }
      batchData.next();
    }
  }

  private static void v2(BatchData batchData) throws IOException {
    PublicBAOS baos = new PublicBAOS();
    Encoder encoder = new DoublePrecisionEncoder();

    while (batchData.hasCurrent()) {
      encoder.encode(batchData.getDouble(), baos);
      batchData.next();
    }
    encoder.flush(baos);

    v2 = baos.size();
    System.out.printf("V2 after encoding: %d, %f\n", v2, v2 / (double) bytesLengthBeforeEncoding);

    ByteBuffer byteBuffer = ByteBuffer.wrap(baos.getBuf(), 0, baos.size());
    batchData.resetBatchData();
    DoublePrecisionDecoder decoder = new DoublePrecisionDecoder();
    while (decoder.hasNext(byteBuffer)) {
      if (decoder.readDouble(byteBuffer) != batchData.getDouble()) {
        throw new RuntimeException();
      }
      batchData.next();
    }
  }
}

class PublicValueCompressor extends ValueCompressor {

  private BitOutput out;

  public PublicValueCompressor(BitOutput out, Predictor predictor) {
    super(out, predictor);
    this.out = out;
  }

  public void add(double value) {
    super.compressValue(Double.doubleToRawLongBits(value));
  }

  public void close() {
    out.writeBits(0x0F, 4);
    out.writeBits(0xFFFFFFFF, 32);
    out.skipBit();
    out.flush();
  }
}
