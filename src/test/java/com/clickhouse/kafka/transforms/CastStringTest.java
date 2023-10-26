/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clickhouse.kafka.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class CastStringTest {
    private final CastArray<SourceRecord> xformKey = new CastArray.Key<>();
    private final CastArray<SourceRecord> xformValue = new CastArray.Value<>();
    private static final long MILLIS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);

    @AfterEach
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test
    public void testConfigEmpty() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(CastArray.SPEC_CONFIG, "")));
    }

    @Test
    public void castNullValueRecordWithSchema() {
        xformValue.configure(Collections.singletonMap(CastArray.SPEC_CONFIG, "foo"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
            Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, null);
        SourceRecord transformed = xformValue.apply(original);
        assertEquals(original, transformed);
    }

    @Test
    public void castNullValueRecordSchemaless() {
        xformValue.configure(Collections.singletonMap(CastArray.SPEC_CONFIG, "foo"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
            Schema.STRING_SCHEMA, "key", null, null);
        SourceRecord transformed = xformValue.apply(original);
        assertEquals(original, transformed);
    }

    @Test
    public void castNullKeyRecordWithSchema() {
        xformKey.configure(Collections.singletonMap(CastArray.SPEC_CONFIG, "foo"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
            Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "value");
        SourceRecord transformed = xformKey.apply(original);
        assertEquals(original, transformed);
    }

    @Test
    public void castNullKeyRecordSchemaless() {
        xformKey.configure(Collections.singletonMap(CastArray.SPEC_CONFIG, "foo"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
            null, null, Schema.STRING_SCHEMA, "value");
        SourceRecord transformed = xformKey.apply(original);
        assertEquals(original, transformed);
    }

//    @Test
//    public void castFieldsWithSchema() {
//        Date day = new Date(MILLIS_PER_DAY);
//        byte[] byteArray = new byte[] {(byte) 0xFE, (byte) 0xDC, (byte) 0xBA, (byte) 0x98, 0x76, 0x54, 0x32, 0x10};
//        ByteBuffer byteBuffer = ByteBuffer.wrap(Arrays.copyOf(byteArray, byteArray.length));
//
//        xformValue.configure(Collections.singletonMap(CastArray.SPEC_CONFIG,
//                "foo, bar, baz"));
//
//        // Include an optional fields and fields with defaults to validate their values are passed through properly
//        SchemaBuilder builder = SchemaBuilder.struct();
//        builder.field("foo", SchemaBuilder.array(SchemaBuilder.map()));
//        builder.field("bar", Schema.OPTIONAL_INT16_SCHEMA);
//        builder.field("baz", SchemaBuilder.int32().defaultValue(2).build());
//
//        Schema supportedTypesSchema = builder.build();
//
//        Struct recordValue = new Struct(supportedTypesSchema);
//        recordValue.put("int8", (byte) 8);
//        recordValue.put("int16", (short) 16);
//        recordValue.put("int32", 32);
//
//        // optional field intentionally omitted
//
//        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
//                supportedTypesSchema, recordValue));
//
//        assertEquals((short) 8, ((Struct) transformed.value()).get("int8"));
//        assertTrue(((Struct) transformed.value()).schema().field("int16").schema().isOptional());
//        assertEquals(2L, ((Struct) transformed.value()).schema().field("int32").schema().defaultValue());
//
//        assertNull(((Struct) transformed.value()).get("optional"));
//
//        Schema transformedSchema = ((Struct) transformed.value()).schema();
//        assertEquals(Schema.INT16_SCHEMA.type(), transformedSchema.field("int8").schema().type());
//        assertEquals(Schema.OPTIONAL_INT32_SCHEMA.type(), transformedSchema.field("int16").schema().type());
//        assertEquals(Schema.INT64_SCHEMA.type(), transformedSchema.field("int32").schema().type());
//
//    }

    @SuppressWarnings("unchecked")
    @Test
    public void castFieldsSchemaless() throws JsonProcessingException {
        xformValue.configure(Collections.singletonMap(CastArray.SPEC_CONFIG, "foo"));
        Map<String, Object> recordValue = new HashMap<>();
        recordValue.put("foo", new ObjectMapper().readValue("{\"sample\":\"object\"}", HashMap.class));
        recordValue.put("bar", new ObjectMapper().readValue("[{\"sample\":\"object\"}]", List.class));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, recordValue));

        assertNull(transformed.valueSchema());
        assertTrue((((Map<String, Object>) transformed.value()).get("foo")) instanceof List);
        List<Object> fooValue = (List<Object>)((Map<String, Object>) transformed.value()).get("foo");
        assertEquals(1, fooValue.size());
        assertTrue(fooValue.get(0) instanceof Map);
        assertTrue((((Map<String, Object>) transformed.value()).get("bar")) instanceof List);
        List<Object> barValue = (List<Object>)((Map<String, Object>) transformed.value()).get("bar");
        assertEquals(1, barValue.size());
        assertTrue(barValue.get(0) instanceof Map);
    }


    @Test
    public void testCastVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xformKey.version());
        assertEquals(AppInfoParser.getVersion(), xformValue.version());

        assertEquals(xformKey.version(), xformValue.version());
    }

}
