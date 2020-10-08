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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.encoding.encoder.DoublePrecisionEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.ExternalGorillaEncoder;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.junit.Before;
import org.junit.Test;

public class GorillaBenchmark {

  private final List<Double> values = new ArrayList<>();

  private ByteBuffer internal;
  private ByteBuffer external;

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
    Encoder encoder = new DoublePrecisionEncoder();
    PublicBAOS publicBAOS = new PublicBAOS();

    long start = System.currentTimeMillis();
    for (Double value : values) {
      encoder.encode(value, publicBAOS);
    }
    encoder.flush(publicBAOS);
    long end = System.currentTimeMillis();
    long cost0 = end - start;
    System.out.println(cost0);
    internal = ByteBuffer.wrap(publicBAOS.getBuf());

    encoder = new ExternalGorillaEncoder();
    publicBAOS = new PublicBAOS();

    start = System.currentTimeMillis();
    for (Double value : values) {
      encoder.encode(value, publicBAOS);
    }
    end = System.currentTimeMillis();
    encoder.flush(publicBAOS);
    long cost1 = end - start;
    System.out.println(cost1);
    ((ExternalGorillaEncoder) encoder).getByteBuffer().flip();
    external = ((ExternalGorillaEncoder) encoder).getByteBuffer();

    System.out.println((double) cost1 / cost0);
  }

  @Test
  public void testDecoding() throws IOException {
    Decoder decoder = new DoublePrecisionDecoder();
    long start = System.currentTimeMillis();
    for (Double value : values) {
      decoder.readDouble(internal);
    }
    long end = System.currentTimeMillis();
    long cost0 = end - start;
    System.out.println(cost0);

    decoder = new ExternalGorillaDecoder(external);
    start = System.currentTimeMillis();
    for (Double value : values) {
      decoder.readDouble(external);
    }
    end = System.currentTimeMillis();
    long cost1 = end - start;
    System.out.println(cost1);

    System.out.println((double) cost1 / cost0);
  }
}
