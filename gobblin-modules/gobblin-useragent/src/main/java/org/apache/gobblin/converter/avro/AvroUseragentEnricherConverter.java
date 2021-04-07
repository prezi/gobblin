/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.converter.avro;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import io.github.mngsk.devicedetector.Detection;
import io.github.mngsk.devicedetector.client.Client;
import io.github.mngsk.devicedetector.device.Device;
import io.github.mngsk.devicedetector.operatingsystem.OperatingSystem;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.ForkOperatorUtils;

import io.github.mngsk.devicedetector.DeviceDetector;
import io.github.mngsk.devicedetector.DeviceDetector.DeviceDetectorBuilder;


/**
 * Converts Avro record to Json record
 *
 * @author nveeramr
 *
 */
public class AvroUseragentEnricherConverter extends Converter<Schema, Schema, GenericRecord, GenericRecord> {
    public static final String CONVERTER_AVRO_USERAGENT_ENRICHER_UA_FIELD = "converter.avro.useragent.enricher.ua.field";
    public static final String CONVERTER_AVRO_USERAGENT_ENRICHER_REMOVE_UA_FIELD = "converter.avro.useragent.enricher.remove.ua.field";

    private String uaField;

    private static final String UA_STRING_FIELD_NAME = "ua_string";
    private static final String UA_TYPE_FIELD_NAME = "ua_type";
    private static final String DEVICE_FAMILY_FIELD_NAME = "device_family";
    private static final String OS_FAMILY_FIELD_NAME = "os_family";
    private static final String OS_MAJOR_VERSION_FIELD_NAME = "os_major_version";
    private static final String OS_VERSION_STRING_FIELD_NAME = "os_version_string";
    private static final String BROWSER_FAMILY_FIELD_NAME = "browser_family";
    private static final String BROWSER_MAJOR_VERSION_FIELD_NAME = "browser_major_version";
    private static final String BROWSER_VERSION_STRING_FIELD_NAME = "browser_version_string";
    private static final String BROWSER_BIT_FIELD_NAME = "browser_bit";
    private static final String IS_BOT_FIELD_NAME = "is_bot";
    private String parent = null;
    private boolean removeUaField;

    private DeviceDetector deviceDetector;

    private final static Map<String, String> uaTypeMapping = new HashMap<String, String>() {
        {
            put("desktop", "DESKTOP");
            put("feature phone", "MOBILE");
            put("smartphone", "MOBILE");
            put("phablet", "MOBILE");
            put("tablet", "TABLET");
            put("console", "UNKNOWN");
            put("tv", "UNKNOWN");
            put("car browser", "UNKNOWN");
            put("smart display", "UNKNOWN");
            put("camera", "UNKNOWN");
            put("portable media player", "UNKNOWN");
            put("smart speaker", "UNKNOWN");
        }};


    @Override
    public Converter<Schema, Schema, GenericRecord, GenericRecord> init(WorkUnitState workUnit) {
        Preconditions.checkArgument(workUnit.contains(ForkOperatorUtils.getPropertyNameForBranch(workUnit,
                CONVERTER_AVRO_USERAGENT_ENRICHER_UA_FIELD)),
                "The converter " + this.getClass().getName() + " cannot be used without setting the property "
                        + CONVERTER_AVRO_USERAGENT_ENRICHER_UA_FIELD);

        this.uaField =
                workUnit.getProp(ForkOperatorUtils.getPropertyNameForBranch(workUnit,
                        CONVERTER_AVRO_USERAGENT_ENRICHER_UA_FIELD));

        this.removeUaField =
                workUnit.getPropAsBoolean(ForkOperatorUtils.getPropertyNameForBranch(workUnit,
                        CONVERTER_AVRO_USERAGENT_ENRICHER_REMOVE_UA_FIELD), false);

        deviceDetector = new DeviceDetectorBuilder().enableEverything().build();

        return this;
    }

    @Override
    public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)
            throws SchemaConversionException {
        Schema outputSchema;
        // Find the field
        Optional<Field> optional = AvroUtils.getField(inputSchema, this.uaField);
        if (!optional.isPresent()) {
            throw new SchemaConversionException("Unable to get field with location: " + this.uaField);
        }

        if (this.uaField.contains(".")){
            this.parent = this.uaField.substring(0, this.uaField.lastIndexOf('.'));
        }
        if (this.parent != null) {
            Field parentField = AvroUtils.getField(inputSchema, this.parent).get();

            List<Field> fields = new ArrayList<>();
            // Clone the existing fields
            for (Field field : inputSchema.getFields()) {
                if (!field.name().equals(parentField.name())) {
                    fields.add(new Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order()));
                }
            }

            Schema enrichedSchema = enrichSchemaWithUseragentData(parentField.schema(), parentField.schema().getFields());

            Field parentFieldNew = new Field(parentField.name(), enrichedSchema, parentField.doc(), parentField.defaultVal());
            fields.add(parentFieldNew);
            outputSchema = Schema
                    .createRecord(inputSchema.getName(), inputSchema.getDoc(), inputSchema.getNamespace(), inputSchema.isError());
            outputSchema.setFields(fields);

        } else {
            outputSchema =  enrichSchemaWithUseragentData(inputSchema, inputSchema.getFields());
        }

        return outputSchema;
    }

    private String removeLineBreaksAndTabs(String useragent) {
        return useragent
                .replace("\r\n", " ")
                .replace("\r", " ")
                .replace("\n", " ")
                .replace("\t", " ");
    }

    private Optional<String> getVersionPart(String version, Integer partIndex) {
        try {
            String[] parts = version.split("\\.");
            if (parts.length > partIndex) {
                return Optional.of(parts[partIndex]);
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private Schema enrichSchemaWithUseragentData(Schema inputSchema, List<Field> inputFields) {
        List<Field> fields = new ArrayList<>();
        String uaFieldName = this.uaField.substring(this.uaField.lastIndexOf('.')+1, this.uaField.length());

        for (Field field : inputFields) {
            if (!(removeUaField && field.name().equals(uaFieldName))) {
                fields.add(new Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order()));
            }
        }

        fields.add(new Field(UA_STRING_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "Cleaned Useragent String", Schema.NULL_VALUE, Field.Order.ASCENDING));
        fields.add(new Field(UA_TYPE_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "Useragent Type", Schema.NULL_VALUE, Field.Order.ASCENDING));
        fields.add(new Field(DEVICE_FAMILY_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "Device Family", Schema.NULL_VALUE, Field.Order.ASCENDING));
        fields.add(new Field(OS_FAMILY_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "OS Family", Schema.NULL_VALUE, Field.Order.ASCENDING));
        fields.add(new Field(OS_MAJOR_VERSION_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "OS Major Version", Schema.NULL_VALUE, Field.Order.ASCENDING));
        fields.add(new Field(OS_VERSION_STRING_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "OS Version String", Schema.NULL_VALUE, Field.Order.ASCENDING));
        fields.add(new Field(BROWSER_FAMILY_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "Browser Family", Schema.NULL_VALUE, Field.Order.ASCENDING));
        fields.add(new Field(BROWSER_MAJOR_VERSION_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "Browser Major Version", Schema.NULL_VALUE, Field.Order.ASCENDING));
        fields.add(new Field(BROWSER_VERSION_STRING_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "Browser Version String", Schema.NULL_VALUE, Field.Order.ASCENDING));
        fields.add(new Field(BROWSER_BIT_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "Browser Bit", Schema.NULL_VALUE, Field.Order.ASCENDING));
        fields.add(new Field(IS_BOT_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.BOOLEAN)), "Is Bot", Schema.NULL_VALUE, Field.Order.ASCENDING));

        Schema enrichedSchema = Schema
                .createRecord(inputSchema.getName(), inputSchema.getDoc(), inputSchema.getNamespace(), inputSchema.isError());
        enrichedSchema.setFields(fields);

        return enrichedSchema;

    }

    private void enrichWithUseragent(String useragent, GenericRecord record) {
        String uaString = null;
        String uaType = null;
        String deviceFamily = null;
        String osFamily = null;
        String osMajorVersion = null;
        String osVersionString = null;
        String browserFamily = null;
        String browserMajorVersion = null;
        String browserVersionString = null;
        String browserBit = null;
        Boolean isBot = null;

        if (useragent != null) {
            try {
                uaString = java.net.URLDecoder.decode(useragent, StandardCharsets.UTF_8.name()).replace('+', ' ');
                Detection detection = deviceDetector.detect(removeLineBreaksAndTabs(uaString));
                uaType = getUaType(detection);

                String brand = detection.getDevice()
                        .flatMap(Device::getBrand)
                        .orElse(null);
                String model = detection.getDevice()
                        .flatMap(Device::getModel)
                        .orElse(null);
                if (brand != null && !brand.equals("UNK")) {
                    if (model != null) {
                        deviceFamily = brand + " " + model;
                    } else {
                        deviceFamily = brand;
                    }
                } else {
                    deviceFamily = "Other";
                }

                osFamily = detection
                        .getOperatingSystem()
                        .flatMap(OperatingSystem::getFamily)
                        .orElse("");
                if (osFamily.equals("GNU/Linux")) {
                    osFamily = "Linux";
                }

                osMajorVersion = detection
                        .getOperatingSystem()
                        .flatMap(OperatingSystem::getVersion)
                        .flatMap(x -> getVersionPart(x, 0))
                        .orElse("");
                osVersionString = detection
                        .getOperatingSystem()
                        .flatMap(OperatingSystem::getVersion)
                        .orElse("");
                browserFamily = detection
                        .getClient()
                        .flatMap(Client::getName)
                        .orElse("");
                browserMajorVersion = detection
                        .getClient()
                        .flatMap(Client::getVersion)
                        .flatMap(x -> getVersionPart(x, 0))
                        .orElse("");
                browserVersionString = detection
                        .getClient()
                        .flatMap(Client::getVersion)
                        .orElse("");
                browserBit = detection
                        .getOperatingSystem()
                        .flatMap(OperatingSystem::getPlatform)
                        .map(x -> x.equals("x86") ? "32" : "64")
                        .orElse("64");
                isBot = detection.isBot();
            } catch (UnsupportedEncodingException e) {

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            record.put(UA_STRING_FIELD_NAME, uaString);
            record.put(UA_TYPE_FIELD_NAME, uaType);
            record.put(DEVICE_FAMILY_FIELD_NAME, deviceFamily);
            record.put(OS_FAMILY_FIELD_NAME, osFamily);
            record.put(OS_MAJOR_VERSION_FIELD_NAME, osMajorVersion);
            record.put(OS_VERSION_STRING_FIELD_NAME, osVersionString);
            record.put(BROWSER_FAMILY_FIELD_NAME, browserFamily);
            record.put(BROWSER_MAJOR_VERSION_FIELD_NAME, browserMajorVersion);
            record.put(BROWSER_VERSION_STRING_FIELD_NAME, browserVersionString);
            record.put(BROWSER_BIT_FIELD_NAME, browserBit);
            record.put(IS_BOT_FIELD_NAME, isBot);
        }
    }

    private String getUaType(Detection detection) {
        if (detection.isBot()) {
            return "BOT";
        }

        java.util.Optional<Device> device = detection.getDevice();
        return uaTypeMapping.getOrDefault(device.map(Device::getType).orElse(""), "UNKNOWN");
    }

    private GenericRecord copyRecords (GenericRecord record, Schema outputSchema, Iterator<String> levels)
    {
        String level = levels.next();

        GenericRecord outputRecord = new GenericData.Record(outputSchema);

        if (!levels.hasNext()) {
            if (AvroUtils.getFieldValue(record, level).isPresent()) {
                enrichWithUseragent(AvroUtils.getFieldValue(record, level).get().toString(), outputRecord);
            }else {
                enrichWithUseragent(null, outputRecord);
            }
        }
        for (Field field : outputSchema.getFields()) {
            if (field.name().equals(level) && levels.hasNext()) {
                Schema levelSchema = field.schema();
                GenericRecord newRecord = copyRecords((GenericRecord) record.get(field.name()), levelSchema, levels);
                outputRecord.put( field.name(), newRecord);
            } else {
                if (outputRecord.get(field.name()) == null) {
                    outputRecord.put(field.name(), record.get(field.name()));
                }
            }
        }
        return outputRecord;
    }

    @Override
    public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit) {
        Iterator<String> levels = Splitter.on(".").split(uaField).iterator();
        GenericRecord record = copyRecords(inputRecord, outputSchema, levels);

        return new SingleRecordIterable<>(record);
    }
}