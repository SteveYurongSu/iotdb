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

package org.apache.iotdb.tsfile.encoding.decoder;

import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;
import fi.iki.yak.ts.compression.gorilla.GorillaCompressor;
import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import fi.iki.yak.ts.compression.gorilla.Pair;
import fi.iki.yak.ts.compression.gorilla.predictors.LastValuePredictor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder.LongDeltaDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder.LongDeltaEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.ExternalGorillaEncoder;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.junit.Before;
import org.junit.Test;

public class GorillaBenchmark {

  private final List<Double> values = new ArrayList<>();

  private ByteBuffer internal;
  private ByteBuffer external;
  ByteBufferBitOutput compressedOutput;

  @Before
  public void setUp() throws Exception {
    double x = 0;
    for (int i = 0; i < 10000000; ++i) {
      values.add(Math.sin(x));
      x += 0.00001;
    }
    testEncoding();
  }

  public void testEncoding() throws IOException {
    Encoder encoder = new LongDeltaEncoder();
    PublicBAOS publicBAOS = new PublicBAOS();

    long start = System.currentTimeMillis();
    for (long i = 0; i < 10000000; ++i) {
      encoder.encode(i, publicBAOS);
    }
    encoder.flush(publicBAOS);
    long end = System.currentTimeMillis();
    long cost0 = end - start;
    System.out.println(cost0);
    internal = ByteBuffer.wrap(publicBAOS.getBuf());

    encoder = new ExternalGorillaEncoder();
    publicBAOS = new PublicBAOS();

//    start = System.currentTimeMillis();
//    for (int i = 0; i < 10000000; ++i) {
//      encoder.encode(values.get(i), publicBAOS);
//    }
//    end = System.currentTimeMillis();
//    encoder.flush(publicBAOS);
//    long cost1 = end - start;
//    System.out.println(cost1);
//    ((ExternalGorillaEncoder) encoder).getByteBuffer().flip();
//    external = ((ExternalGorillaEncoder) encoder).getByteBuffer();

    compressedOutput = new ByteBufferBitOutput();
    GorillaCompressor compressor = new GorillaCompressor(0, compressedOutput,
        new LastValuePredictor());
    start = System.currentTimeMillis();
    for (int i = 0; i < 10000000; ++i) {
      compressor.addValue(i, values.get(i));
    }
    compressor.close();
    end = System.currentTimeMillis();
    System.out.println(end - start);
//    System.out.println((double) cost1 / cost0);
  }

  @Test
  public void testDecoding() throws IOException {
    Decoder decoder = new LongDeltaDecoder();
    long start = System.currentTimeMillis();
    for (Double value : values) {
      decoder.readLong(internal);
    }
    long end = System.currentTimeMillis();
    long cost0 = end - start;
    System.out.println(cost0);

    compressedOutput.getByteBuffer().flip();
    GorillaDecompressor decompressor = new GorillaDecompressor(
        new ByteBufferBitInput(compressedOutput.getByteBuffer()), new LastValuePredictor());
    start = System.currentTimeMillis();
    Pair pair;
    while ((pair = decompressor.readPair()) != null) {
      pair.getTimestamp();
      pair.getDoubleValue();
    }
    end = System.currentTimeMillis();
    long cost1 = end - start;
    System.out.println(cost1);

    System.out.println((double) cost1 / cost0);
  }
}
