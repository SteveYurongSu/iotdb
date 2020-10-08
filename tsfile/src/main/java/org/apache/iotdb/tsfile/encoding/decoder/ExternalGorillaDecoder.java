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
import fi.iki.yak.ts.compression.gorilla.ValueDecompressor;
import fi.iki.yak.ts.compression.gorilla.predictors.LastValuePredictor;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ExternalGorillaDecoder extends Decoder {

  private ByteBuffer byteBuffer;
  private LastValuePredictor predictor;
  private ValueDecompressor decompressor;

  private boolean firstWasRead;

  private int count;

  public ExternalGorillaDecoder(ByteBuffer byteBuffer) {
    super(null);
    this.byteBuffer = byteBuffer;
    predictor = new LastValuePredictor();
    decompressor = new ValueDecompressor(
        new ByteBufferBitInput(byteBuffer), new LastValuePredictor());
    firstWasRead = false;
    count = 0;
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    return count < 1000_0000;
  }

  @Override
  public void reset() {
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    return Double.longBitsToDouble(decompressor.nextValue());
  }
}
