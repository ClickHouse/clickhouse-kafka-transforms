package com.clickhouse.kafka.transforms;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.UUID.randomUUID;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class CastString<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    private static final Logger LOGGER = LoggerFactory.getLogger(CastString.class);

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("wrapper.field", ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
                @SuppressWarnings("unchecked")
                @Override
                public void ensureValid(String name, Object valueObject) {
                    String value = (String) valueObject;
                    if (value == null || value.isEmpty()) {
                        throw new ConfigException("Must specify one name for the wrapper field.");
                    }
                }

                @Override
                public String toString() {
                    return "name of wrapper field, e.g. <code>foo</code>";
                }
            },
            ConfigDef.Importance.HIGH,
            "Name of field");

    private static final String PURPOSE = "cast record";
    private static String wrapperField;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        wrapperField = config.getString("wrapper.field");
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
        LOGGER.trace("Record type: {}", record.getClass());
        return newRecord(record, null, castValueToEscapedString(null, value));
    }

    private R applyWithSchema(R record) {
        Schema valueSchema = operatingSchema(record);
        Schema updatedSchema = getOrBuildSchema(valueSchema);

        // Casting within a struct
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        final Struct updatedValue = new Struct(updatedSchema);
        updatedValue.put(updatedSchema.field(wrapperField), castValueToEscapedString(updatedSchema, value));
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema getOrBuildSchema(Schema valueSchema) {
        final SchemaBuilder builder;

        builder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());
        builder.field(wrapperField, Schema.STRING_SCHEMA);
        return builder.build();
    }


    private static Map<String, Object> castValueToEscapedString(Schema schema, Object value) {
        if (value == null) return null;

        Schema.Type inferredType = schema == null ? ConnectSchema.schemaType(value.getClass()) : schema.type();
        if (inferredType == null) {
            throw new DataException("Cast transformation was passed a value of type " + value.getClass()
                    + " which is not supported by Connect's data API");
        }

        Gson gson = new Gson();
        java.lang.reflect.Type gsonType = new TypeToken<HashMap<String, String>>() {}.getType();
        String castedValue = gson.toJson(value, new TypeToken<HashMap>() {}.getType());
        castedValue = castedValue.replace("\\", "\\\\");
        castedValue = castedValue.replace("\"", "\\\"");
        castedValue = "{\"" + wrapperField + "\": \"" + castedValue + "\"}";
        LOGGER.trace("Cast value '{}' to string '{}'", value, castedValue);
        LOGGER.trace("Cast value '{}' to object '{}'", value, gson.fromJson(castedValue, gsonType));
        return gson.fromJson(castedValue, gsonType);
    }


    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static final class Key<R extends ConnectRecord<R>> extends CastString<R> {
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

    public static final class Value<R extends ConnectRecord<R>> extends CastString<R> {
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