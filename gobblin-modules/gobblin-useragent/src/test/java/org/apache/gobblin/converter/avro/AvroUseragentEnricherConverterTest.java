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

package java.org.apache.gobblin.converter.avro;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.gobblin.converter.avro.AvroUseragentEnricherConverter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import junit.framework.Assert;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.util.AvroUtils;

import static org.apache.gobblin.converter.avro.AvroUseragentEnricherConverter.CONVERTER_AVRO_USERAGENT_ENRICHER_UA_FIELD;
import static org.apache.gobblin.converter.avro.AvroUseragentEnricherConverter.CONVERTER_AVRO_USERAGENT_ENRICHER_REMOVE_UA_FIELD;


public class AvroUseragentEnricherConverterTest {

    @BeforeMethod
    public void setUp() {
    }

    @Test
    public void testEnrichAvroWithUseragentSuccessfully()
            throws IOException {
        Schema inputSchema = new Schema.Parser()
                .parse(getClass().getClassLoader().getResourceAsStream("input.avsc"));

        AvroUseragentEnricherConverter converter = new AvroUseragentEnricherConverter();
        WorkUnitState state = new WorkUnitState();
        state.setProp(CONVERTER_AVRO_USERAGENT_ENRICHER_UA_FIELD, "user_agent");
        state.setProp(CONVERTER_AVRO_USERAGENT_ENRICHER_REMOVE_UA_FIELD, false);

        converter.init(state);
        Schema outputSchema = converter.convertSchema(inputSchema, state);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(inputSchema);

        File tmp = File.createTempFile(this.getClass().getSimpleName(), null);

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("input.avro");
        FileUtils.copyInputStreamToFile(inputStream, tmp);

        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(tmp, datumReader);
        GenericRecord inputRecord = dataFileReader.next();

        Iterable<GenericRecord> genericRecords = converter.convertRecord(outputSchema, inputRecord, state);
        GenericRecord outputRecord = genericRecords.iterator().next();

        Assert.assertEquals("Mozilla/5.0 (Linux; Android 7.0; SM-P585 Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36", AvroUtils.getFieldValue(outputRecord, "user_agent").get().toString());
        Assert.assertEquals("Mozilla/5.0 (Linux; Android 7.0; SM-P585 Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36", AvroUtils.getFieldValue(outputRecord, "ua_string").get().toString());
        Assert.assertEquals("TABLET", AvroUtils.getFieldValue(outputRecord, "ua_type").get().toString());
        Assert.assertEquals("\"Samsung GALAXY Tab A 10.1\\\" WiFi (2016)\"", AvroUtils.getFieldValue(outputRecord, "device_family").get().toString());
        Assert.assertEquals("Android", AvroUtils.getFieldValue(outputRecord, "os_family").get().toString());
        Assert.assertEquals("7", AvroUtils.getFieldValue(outputRecord, "os_major_version").get().toString());
        Assert.assertEquals("7.0", AvroUtils.getFieldValue(outputRecord, "os_version_string").get().toString());
        Assert.assertEquals("Chrome", AvroUtils.getFieldValue(outputRecord, "browser_family").get().toString());
        Assert.assertEquals("56", AvroUtils.getFieldValue(outputRecord, "browser_major_version").get().toString());
        Assert.assertEquals("56.0.2924.87", AvroUtils.getFieldValue(outputRecord, "browser_version_string").get().toString());
        Assert.assertEquals("64", AvroUtils.getFieldValue(outputRecord, "browser_bit").get().toString());
        Assert.assertEquals(false, AvroUtils.getFieldValue(outputRecord, "is_bot").get());
    }

    @Test
    public void testEnrichAvroWithNullFieldGivesNullUaFields()
            throws DataConversionException, IOException, SchemaConversionException {
        Schema inputSchema = new Schema.Parser()
                .parse(getClass().getClassLoader().getResourceAsStream("input.avsc"));

        AvroUseragentEnricherConverter converter = new AvroUseragentEnricherConverter();
        WorkUnitState state = new WorkUnitState();
        state.setProp(CONVERTER_AVRO_USERAGENT_ENRICHER_UA_FIELD, "user_agent");
        state.setProp(CONVERTER_AVRO_USERAGENT_ENRICHER_REMOVE_UA_FIELD, false);

        converter.init(state);
        Schema outputSchema = converter.convertSchema(inputSchema, state);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(inputSchema);

        File tmp = File.createTempFile(this.getClass().getSimpleName(), null);

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("null_input.avro");
        FileUtils.copyInputStreamToFile(inputStream, tmp);

        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(tmp, datumReader);
        GenericRecord inputRecord = dataFileReader.next();

        Iterable<GenericRecord> genericRecords = converter.convertRecord(outputSchema, inputRecord, state);
        GenericRecord outputRecord = genericRecords.iterator().next();

        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "user_agent").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "ua_string").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "ua_type").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "device_family").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "os_family").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "os_major_version").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "os_version_string").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "browser_family").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "browser_major_version").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "browser_version_string").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "browser_bit").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "is_bot").isPresent());
    }

    @Test
    public void testEnrichAvroWithUseragentAndRemoveSourceFieldSuccessfully()
            throws DataConversionException, IOException {
        Schema inputSchema = new Schema.Parser()
                .parse(getClass().getClassLoader().getResourceAsStream("input.avsc"));

        AvroUseragentEnricherConverter converter = new AvroUseragentEnricherConverter();
        WorkUnitState state = new WorkUnitState();
        state.setProp(CONVERTER_AVRO_USERAGENT_ENRICHER_UA_FIELD, "user_agent");
        state.setProp(CONVERTER_AVRO_USERAGENT_ENRICHER_REMOVE_UA_FIELD, true);

        converter.init(state);
        Schema outputSchema = converter.convertSchema(inputSchema, state);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(inputSchema);

        File tmp = File.createTempFile(this.getClass().getSimpleName(), null);

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("input.avro");
        FileUtils.copyInputStreamToFile(inputStream, tmp);

        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(tmp, datumReader);
        GenericRecord inputRecord = dataFileReader.next();

        Iterable<GenericRecord> genericRecords = converter.convertRecord(outputSchema, inputRecord, state);
        GenericRecord outputRecord = genericRecords.iterator().next();

        Assert.assertEquals("Mozilla/5.0 (Linux; Android 7.0; SM-P585 Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36", AvroUtils.getFieldValue(outputRecord, "ua_string").get().toString());
        Assert.assertEquals("TABLET", AvroUtils.getFieldValue(outputRecord, "ua_type").get().toString());
        Assert.assertEquals("\"Samsung GALAXY Tab A 10.1\\\" WiFi (2016)\"", AvroUtils.getFieldValue(outputRecord, "device_family").get().toString());
        Assert.assertEquals("Android", AvroUtils.getFieldValue(outputRecord, "os_family").get().toString());
        Assert.assertEquals("7", AvroUtils.getFieldValue(outputRecord, "os_major_version").get().toString());
        Assert.assertEquals("7.0", AvroUtils.getFieldValue(outputRecord, "os_version_string").get().toString());
        Assert.assertEquals("Chrome", AvroUtils.getFieldValue(outputRecord, "browser_family").get().toString());
        Assert.assertEquals("56", AvroUtils.getFieldValue(outputRecord, "browser_major_version").get().toString());
        Assert.assertEquals("56.0.2924.87", AvroUtils.getFieldValue(outputRecord, "browser_version_string").get().toString());
        Assert.assertEquals("64", AvroUtils.getFieldValue(outputRecord, "browser_bit").get().toString());
        Assert.assertEquals(false, AvroUtils.getFieldValue(outputRecord, "is_bot").get());
    }

    @Test
    public void testEnrichAvroWithNullFieldAndRemovingSourceFieldGivesNullUaFields()
            throws DataConversionException, IOException, SchemaConversionException {
        Schema inputSchema = new Schema.Parser()
                .parse(getClass().getClassLoader().getResourceAsStream("input.avsc"));

        AvroUseragentEnricherConverter converter = new AvroUseragentEnricherConverter();
        WorkUnitState state = new WorkUnitState();
        state.setProp(CONVERTER_AVRO_USERAGENT_ENRICHER_UA_FIELD, "user_agent");
        state.setProp(CONVERTER_AVRO_USERAGENT_ENRICHER_REMOVE_UA_FIELD, true);

        converter.init(state);
        Schema outputSchema = converter.convertSchema(inputSchema, state);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(inputSchema);

        File tmp = File.createTempFile(this.getClass().getSimpleName(), null);

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("null_input.avro");
        FileUtils.copyInputStreamToFile(inputStream, tmp);

        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(tmp, datumReader);
        GenericRecord inputRecord = dataFileReader.next();

        Iterable<GenericRecord> genericRecords = converter.convertRecord(outputSchema, inputRecord, state);
        GenericRecord outputRecord = genericRecords.iterator().next();

        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "user_agent").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "ua_string").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "ua_type").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "device_family").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "os_family").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "os_major_version").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "os_version_string").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "browser_family").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "browser_major_version").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "browser_version_string").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "browser_bit").isPresent());
        Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "is_bot").isPresent());
    }
}


