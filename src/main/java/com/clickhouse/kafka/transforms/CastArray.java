package com.clickhouse.kafka.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class CastArray<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    private static final Logger LOGGER = LoggerFactory.getLogger(CastArray.class);

    // TODO: Currently we only support top-level field casting. Ideally we could use a dotted notation in the spec to
    // allow casting nested fields.
    public static final String OVERVIEW_DOC =
            "Cast specified fields with JSON to Array(JSON)";

    public static final String SPEC_CONFIG = "spec";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SPEC_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
                        @SuppressWarnings("unchecked")
                        @Override
                        public void ensureValid(String name, Object valueObject) {
                            List<String> value = (List<String>) valueObject;
                            if (value == null || value.isEmpty()) {
                                throw new ConfigException("Must specify at least one field to cast.");
                            }
                        }

                        @Override
                        public String toString() {
                            return "list of fields, e.g. <code>foo,abc</code>";
                        }
                    },
                    ConfigDef.Importance.HIGH,
                    "List of fields");

    private static final String PURPOSE = "cast types";


    // As a special case for casting the entire value (e.g. the incoming key is a int64 but you know it could be an
    // int32 and want the smaller width), we use an otherwise invalid field name in the cast spec to track this.

    private List<String> fields;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fields = config.getList(SPEC_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        }

        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }


    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final HashMap<String, Object> updatedValue = new HashMap<>(value);

        LOGGER.debug("Cast fields '{}'", fields);
        for (String field : fields) {
            updatedValue.put(field, castValueToArray(null, value.get(field)));
            LOGGER.trace("Cast field '{}' from '{}' to '{}'", field, value.get(field), updatedValue.get(field));
        }
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        Schema valueSchema = operatingSchema(record);
        Schema updatedSchema = getOrBuildSchema(valueSchema);

        // Casting within a struct
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object origFieldValue = value.get(field);
            final Object newFieldValue = castValueToArray(field.schema(), origFieldValue);
            LOGGER.trace("Cast field '{}' from '{}' to '{}'", field.name(), origFieldValue, newFieldValue);
            updatedValue.put(updatedSchema.field(field.name()), newFieldValue);
        }
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema getOrBuildSchema(Schema valueSchema) {
        final SchemaBuilder builder;

        builder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());
        for (Field field : valueSchema.fields()) {
            if (fields.contains(field.name())) {
                Schema fieldSchema = field.schema();
                SchemaBuilder fieldBuilder = SchemaBuilder.array(fieldSchema);
                if (fieldSchema.isOptional())
                    fieldBuilder.optional();
                if (fieldSchema.defaultValue() != null) {
                    fieldBuilder.defaultValue(castValueToArray(fieldSchema, fieldSchema.defaultValue()));
                }
                builder.field(field.name(), fieldBuilder.build());
            } else {
                builder.field(field.name(), field.schema());
            }
        }


        if (valueSchema.isOptional())
            builder.optional();
        if (valueSchema.defaultValue() != null)
            builder.defaultValue(castValueToArray(valueSchema, valueSchema.defaultValue()));

        return builder.build();
    }


    private static List castValueToArray(Schema schema, Object value) {
        if (value == null) return null;

        Schema.Type inferredType = schema == null ? ConnectSchema.schemaType(value.getClass()) : schema.type();
        if (inferredType == null) {
            throw new DataException("Cast transformation was passed a value of type " + value.getClass()
                    + " which is not supported by Connect's data API");
        }

        if (!(value instanceof List)) {
            return List.of(value);
        }

        return (List) value;
    }


    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static final class Key<R extends ConnectRecord<R>> extends CastArray<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends CastArray<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

}