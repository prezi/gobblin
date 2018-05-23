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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import junit.framework.Assert;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.util.AvroUtils;

import static org.apache.gobblin.converter.avro.AvroGeoipEnricherConverter.CONVERTER_AVRO_GEOIP_ENRICHER_IP_FIELD;
import static org.apache.gobblin.converter.avro.AvroGeoipEnricherConverter.CONVERTER_AVRO_GEOIP_ENRICHER_MAXMIND_DB_PATH;
import static org.apache.gobblin.converter.avro.AvroGeoipEnricherConverter.CONVERTER_AVRO_GEOIP_ENRICHER_REMOVE_IP_FIELD;


public class AvroGeoipEnricherConverterTest {

  @BeforeMethod
  public void setUp()
      throws Exception {
  }

  @Test
  public void testEnrichEmbededAvroWithGeoipSuccessfully()
      throws DataConversionException, IOException, SchemaConversionException {
    Schema inputSchema = new Schema.Parser()
        .parse(getClass().getClassLoader().getResourceAsStream("schema_embeded.avsc"));

    String maxmindTestDbUri = "file://"+getClass().getClassLoader().getResource("GeoIP2-City-Test.mmdb").getPath();

    AvroGeoipEnricherConverter converter = new AvroGeoipEnricherConverter();
    WorkUnitState state = new WorkUnitState();
    state.setProp(CONVERTER_AVRO_GEOIP_ENRICHER_MAXMIND_DB_PATH, maxmindTestDbUri);
    state.setProp(CONVERTER_AVRO_GEOIP_ENRICHER_IP_FIELD, "test.ip_address");

    converter.init(state);
    Schema outputSchema = converter.convertSchema(inputSchema, state);

    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(inputSchema);

    File tmp = File.createTempFile(this.getClass().getSimpleName(), null);

    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("input_data_embeded.avro");
    FileUtils.copyInputStreamToFile(inputStream, tmp);

    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(tmp, datumReader);
    GenericRecord inputRecord = dataFileReader.next();

    Iterable<GenericRecord> genericRecords = converter.convertRecord(outputSchema, inputRecord, state);
    GenericRecord outputRecord = genericRecords.iterator().next();

    Assert.assertEquals("London", AvroUtils.getFieldValue(outputRecord, "test.city").get().toString());
    Assert.assertEquals("United Kingdom", AvroUtils.getFieldValue(outputRecord, "test.country").get().toString());
    Assert.assertEquals("GB", AvroUtils.getFieldValue(outputRecord, "test.country_code").get().toString());
    Assert.assertEquals("81.2.69.160", AvroUtils.getFieldValue(outputRecord, "test.ip_address").get().toString());

  }

  @Test
  public void testEnrichEmbededAvroWithDroppingIpField()
      throws DataConversionException, IOException, SchemaConversionException {
    Schema inputSchema = new Schema.Parser()
        .parse(getClass().getClassLoader().getResourceAsStream("schema_embeded.avsc"));

    String maxmindTestDbUri = "file://"+getClass().getClassLoader().getResource("GeoIP2-City-Test.mmdb").getPath();

    AvroGeoipEnricherConverter converter = new AvroGeoipEnricherConverter();
    WorkUnitState state = new WorkUnitState();
    state.setProp(CONVERTER_AVRO_GEOIP_ENRICHER_MAXMIND_DB_PATH, maxmindTestDbUri);
    state.setProp(CONVERTER_AVRO_GEOIP_ENRICHER_IP_FIELD, "test.ip_address");
    state.setProp(CONVERTER_AVRO_GEOIP_ENRICHER_REMOVE_IP_FIELD, true);

    converter.init(state);
    Schema outputSchema = converter.convertSchema(inputSchema, state);

    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(inputSchema);

    File tmp = File.createTempFile(this.getClass().getSimpleName(), null);

    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("input_data_embeded.avro");
    FileUtils.copyInputStreamToFile(inputStream, tmp);

    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(tmp, datumReader);
    GenericRecord inputRecord = dataFileReader.next();

    Iterable<GenericRecord> genericRecords = converter.convertRecord(outputSchema, inputRecord, state);
    GenericRecord outputRecord = genericRecords.iterator().next();

    Assert.assertEquals("London", AvroUtils.getFieldValue(outputRecord, "test.city").get().toString());
    Assert.assertEquals("United Kingdom", AvroUtils.getFieldValue(outputRecord, "test.country").get().toString());
    Assert.assertEquals("GB", AvroUtils.getFieldValue(outputRecord, "test.country_code").get().toString());
    Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "test.ip_address").isPresent());

  }

  @Test
  public void testEnrichAvroWithGeoipSuccessfully()
      throws DataConversionException, IOException, SchemaConversionException {
    Schema inputSchema = new Schema.Parser()
        .parse(getClass().getClassLoader().getResourceAsStream("schema_not_embeded.avsc"));

    String maxmindTestDbUri = "file://"+getClass().getClassLoader().getResource("GeoIP2-City-Test.mmdb").getPath();

    AvroGeoipEnricherConverter converter = new AvroGeoipEnricherConverter();
    WorkUnitState state = new WorkUnitState();
    state.setProp(CONVERTER_AVRO_GEOIP_ENRICHER_MAXMIND_DB_PATH, maxmindTestDbUri);
    state.setProp(CONVERTER_AVRO_GEOIP_ENRICHER_IP_FIELD, "ip_address");

    converter.init(state);
    Schema outputSchema = converter.convertSchema(inputSchema, state);

    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(inputSchema);

    File tmp = File.createTempFile(this.getClass().getSimpleName(), null);

    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("input_data_not_embeded.avro");
    FileUtils.copyInputStreamToFile(inputStream, tmp);

    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(tmp, datumReader);
    GenericRecord inputRecord = dataFileReader.next();

    Iterable<GenericRecord> genericRecords = converter.convertRecord(outputSchema, inputRecord, state);
    GenericRecord outputRecord = genericRecords.iterator().next();

    Assert.assertEquals("London", AvroUtils.getFieldValue(outputRecord, "city").get().toString());
    Assert.assertEquals("United Kingdom", AvroUtils.getFieldValue(outputRecord, "country").get().toString());
    Assert.assertEquals("GB", AvroUtils.getFieldValue(outputRecord, "country_code").get().toString());
    Assert.assertEquals("81.2.69.160", AvroUtils.getFieldValue(outputRecord, "ip_address").get().toString());

  }

  @Test
  public void testEnrichAvroWithGeoipWithRemovedIpSuccessfully()
      throws DataConversionException, IOException, SchemaConversionException {
    Schema inputSchema = new Schema.Parser()
        .parse(getClass().getClassLoader().getResourceAsStream("schema_not_embeded.avsc"));

    String maxmindTestDbUri = "file://"+getClass().getClassLoader().getResource("GeoIP2-City-Test.mmdb").getPath();

    AvroGeoipEnricherConverter converter = new AvroGeoipEnricherConverter();
    WorkUnitState state = new WorkUnitState();
    state.setProp(CONVERTER_AVRO_GEOIP_ENRICHER_MAXMIND_DB_PATH, maxmindTestDbUri);
    state.setProp(CONVERTER_AVRO_GEOIP_ENRICHER_IP_FIELD, "ip_address");
    state.setProp(CONVERTER_AVRO_GEOIP_ENRICHER_REMOVE_IP_FIELD, true);

    converter.init(state);
    Schema outputSchema = converter.convertSchema(inputSchema, state);

    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(inputSchema);

    File tmp = File.createTempFile(this.getClass().getSimpleName(), null);

    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("input_data_not_embeded.avro");
    FileUtils.copyInputStreamToFile(inputStream, tmp);

    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(tmp, datumReader);
    GenericRecord inputRecord = dataFileReader.next();

    Iterable<GenericRecord> genericRecords = converter.convertRecord(outputSchema, inputRecord, state);
    GenericRecord outputRecord = genericRecords.iterator().next();

    Assert.assertEquals("London", AvroUtils.getFieldValue(outputRecord, "city").get().toString());
    Assert.assertEquals("United Kingdom", AvroUtils.getFieldValue(outputRecord, "country").get().toString());
    Assert.assertEquals("GB", AvroUtils.getFieldValue(outputRecord, "country_code").get().toString());
    Assert.assertFalse(AvroUtils.getFieldValue(outputRecord, "ip_address").isPresent());
    }
}


