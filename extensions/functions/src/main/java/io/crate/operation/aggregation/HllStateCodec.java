/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.aggregation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipException;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;

import io.crate.execution.engine.aggregation.impl.HyperLogLogPlusPlus;
import io.crate.operation.aggregation.HyperLogLogDistinctAggregation.HllState;

/**
 * Text form of a {@link HllState}, used to hand states to clients and accept them back: base64 of a version
 * byte followed by the deflated HyperLogLogPlusPlus serialization. Deflate roughly halves the register array,
 * whose values are small and repetitive.
 */
final class HllStateCodec {

    private static final byte VERSION = 2;

    private HllStateCodec() {
    }

    static String encode(HllState state) throws IOException {
        assert state.isInitialized() : "only initialized states can be encoded";
        BytesStreamOutput serialized = new BytesStreamOutput();
        state.hyperLogLogPlusPlus.writeTo(serialized);
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        encoded.write(VERSION);
        try (DeflaterOutputStream deflated = new DeflaterOutputStream(encoded)) {
            deflated.write(BytesReference.toBytes(serialized.bytes()));
        }
        return Base64.getEncoder().encodeToString(encoded.toByteArray());
    }

    static HllState decode(String encoded) throws IOException {
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("hyperloglog state is not valid base64", e);
        }
        if (bytes.length == 0 || bytes[0] != VERSION) {
            throw new IllegalArgumentException("unsupported hyperloglog state encoding version");
        }
        byte[] serialized;
        try (InflaterInputStream inflated = new InflaterInputStream(new ByteArrayInputStream(bytes, 1, bytes.length - 1))) {
            serialized = inflated.readAllBytes();
        } catch (ZipException e) {
            throw new IllegalArgumentException("hyperloglog state is corrupt", e);
        }
        HllState state = new HllState();
        state.hyperLogLogPlusPlus = HyperLogLogPlusPlus.readFrom(StreamInput.wrap(serialized));
        return state;
    }

    static int precision(HllState state) {
        return state.hyperLogLogPlusPlus.precision();
    }
}
