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

package org.apache.iotdb.tsfile.encoding.encoder;

import fi.iki.yak.ts.compression.gorilla.BitOutput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;
import fi.iki.yak.ts.compression.gorilla.Predictor;
import fi.iki.yak.ts.compression.gorilla.ValueCompressor;
import fi.iki.yak.ts.compression.gorilla.predictors.LastValuePredictor;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ExternalGorillaEncoder extends Encoder {

  private class PublicValueCompressor extends ValueCompressor {

    private final BitOutput out;

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

  private final ByteBufferBitOutput bitOutput;
  private final Predictor predictor;
  private final PublicValueCompressor compressor;

  private boolean hasValue;

  public ExternalGorillaEncoder() {
    super(null);
    bitOutput = new ByteBufferBitOutput();
    predictor = new LastValuePredictor();
    compressor = new PublicValueCompressor(bitOutput, predictor);
    hasValue = false;
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    if (!hasValue) {
      encodeFirst(value);
      hasValue = true;
      return;
    }
    compressor.add(value);
  }

  protected void encodeFirst(double value) {
    predictor.update(Double.doubleToRawLongBits(value));
    bitOutput.writeBits(Double.doubleToRawLongBits(value), 64);
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    bitOutput.writeBits(0x0F, 4);
    bitOutput.writeBits(0xFFFFFFFF, 32);
    bitOutput.skipBit();
    bitOutput.flush();
  }

  public ByteBuffer getByteBuffer() {
    return bitOutput.getByteBuffer();
  }
}
