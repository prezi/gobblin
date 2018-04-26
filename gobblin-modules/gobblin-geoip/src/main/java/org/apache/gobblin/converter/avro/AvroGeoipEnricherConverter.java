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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.recordaccess.AvroGenericRecordAccessor;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.gobblin.util.HadoopUtils;


/**
 * Converts Avro record to Json record
 *
 * @author nveeramr
 *
 */
public class AvroGeoipEnricherConverter extends Converter<Schema, Schema, GenericRecord, GenericRecord> {
  public static final String CONVERTER_AVRO_GEOIP_ENRICHER_MAXMIND_DB_PATH = "converter.avro.geoip.enricher.maxmind.db.path";
  public static final String CONVERTER_AVRO_GEOIP_ENRICHER_IP_FIELD = "converter.avro.geoip.enricher.ip.field";
  public static final String CONVERTER_AVRO_GEOIP_ENRICHER_REMOVE_IP_FIELD = "converter.avro.geoip.enricher.remove.ip.field";


  private String ipField;

  private static String CITY_FIELD_NAME = "city";
  private static String COUNTRY_FIELD_NAME = "country";
  private static String COUNTRY_CODE_NAME = "country_code";
  private static String SUBDIVISION_FIELD_NAME = "subdivision";
  private String parent = null;
  private DatabaseReader reader;
  private AvroGenericRecordAccessor accessor;
  private boolean removeIpField;

  @Override
  public Converter<Schema, Schema, GenericRecord, GenericRecord> init(WorkUnitState workUnit) {


    Preconditions.checkArgument(workUnit.contains(ForkOperatorUtils.getPropertyNameForBranch(workUnit,
        CONVERTER_AVRO_GEOIP_ENRICHER_MAXMIND_DB_PATH)),
        "The converter " + this.getClass().getName() + " cannot be used without setting the property "
            + CONVERTER_AVRO_GEOIP_ENRICHER_MAXMIND_DB_PATH);

    Preconditions.checkArgument(workUnit.contains(ForkOperatorUtils.getPropertyNameForBranch(workUnit,
        CONVERTER_AVRO_GEOIP_ENRICHER_IP_FIELD)),
        "The converter " + this.getClass().getName() + " cannot be used without setting the property "
            + CONVERTER_AVRO_GEOIP_ENRICHER_IP_FIELD);

    String maxmindDatabasePath =
        workUnit.getProp(ForkOperatorUtils.getPropertyNameForBranch(workUnit,
            CONVERTER_AVRO_GEOIP_ENRICHER_MAXMIND_DB_PATH));

    this.ipField =
        workUnit.getProp(ForkOperatorUtils.getPropertyNameForBranch(workUnit,
            CONVERTER_AVRO_GEOIP_ENRICHER_IP_FIELD));

    this.removeIpField =
        workUnit.getPropAsBoolean(ForkOperatorUtils.getPropertyNameForBranch(workUnit,
            CONVERTER_AVRO_GEOIP_ENRICHER_REMOVE_IP_FIELD), false);


    try {
      Path path = new Path(maxmindDatabasePath);
      FileSystem fs = path.getFileSystem(HadoopUtils.newConfiguration());

      reader = new DatabaseReader.Builder(fs.open(path)).withCache(new CHMCache()).build();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    Schema outputSchema;
    // Find the field
    Optional<Field> optional = AvroUtils.getField(inputSchema, this.ipField);
    if (!optional.isPresent()) {
      throw new SchemaConversionException("Unable to get field with location: " + this.ipField);
    }

    if (this.ipField.contains(".")){
      this.parent = this.ipField.substring(0, this.ipField.lastIndexOf('.'));
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

      Schema enrichedSchema = enrichSchemaWithGeoipData(parentField.schema(), parentField.schema().getFields());

      Field parentFieldNew = new Field(parentField.name(), enrichedSchema, parentField.doc(), parentField.defaultVal());
      fields.add(parentFieldNew);
      outputSchema = Schema
          .createRecord(inputSchema.getName(), inputSchema.getDoc(), inputSchema.getNamespace(), inputSchema.isError());
      outputSchema.setFields(fields);

    } else {
      outputSchema =  enrichSchemaWithGeoipData(inputSchema, inputSchema.getFields());
    }

    return outputSchema;
  }

  private Schema enrichSchemaWithGeoipData(Schema inputSchema, List<Field> inputFields) {
    List<Field> fields = new ArrayList<>();
    String ipFieldName = this.ipField.substring(this.ipField.lastIndexOf('.')+1, this.ipField.length());

    for (Field field : inputFields) {
      if (!(removeIpField && field.name().equals(ipFieldName))) {
        fields.add(new Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order()));
      }
    }

    fields.add(new Field(CITY_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "City",
        Schema.NULL_VALUE, Field.Order.ASCENDING));
    fields.add(new Field(COUNTRY_CODE_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "Country Code", Schema.NULL_VALUE, Field.Order.ASCENDING));
    fields.add(new Field(COUNTRY_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "Country", Schema.NULL_VALUE, Field.Order.ASCENDING));
    fields.add(new Field(SUBDIVISION_FIELD_NAME, Schema.createUnion(Schema.create(Schema.Type.NULL),Schema.create(Schema.Type.STRING)), "Subdivision Name", Schema.NULL_VALUE, Field.Order.ASCENDING));

    Schema enrichedSchema = Schema
        .createRecord(inputSchema.getName(), inputSchema.getDoc(), inputSchema.getNamespace(), inputSchema.isError());
    enrichedSchema.setFields(fields);

    return enrichedSchema;

  }


  private void enrichWithGeoip(String ipAddress, GenericRecord record) {
    try {
      if (ipAddress.isEmpty()) {
        return;
      }

      InetAddress inetAddress = InetAddress.getByName(ipAddress);
      CityResponse response = reader.city(inetAddress);
      record.put(CITY_FIELD_NAME,response.getCity() == null ? null: response.getCity().getName());
      record.put(COUNTRY_FIELD_NAME,response.getCountry() == null ? null: response.getCountry().getName());
      record.put(COUNTRY_CODE_NAME, response.getCountry() == null ? null: response.getCountry().getIsoCode());
      if (response.getSubdivisions().size() > 0) {
        record.put(SUBDIVISION_FIELD_NAME, new Utf8(response.getSubdivisions().get(0).getIsoCode()));
      }

    } catch (GeoIp2Exception e) {
      record.put(CITY_FIELD_NAME, null);
      record.put(COUNTRY_FIELD_NAME, null);
      record.put(COUNTRY_CODE_NAME, null);
      record.put(SUBDIVISION_FIELD_NAME, null);
    } catch (IOException e) {
      e.printStackTrace();
    };
  }

  private GenericRecord copyRecords (GenericRecord record, Schema outputSchema, Iterator<String> levels)
  {
    String level = levels.next();

    GenericRecord outputRecord = new GenericData.Record(outputSchema);

    if (!levels.hasNext()) {
      enrichWithGeoip(AvroUtils.getFieldValue(record, level).get().toString(), outputRecord);
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
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    String subField;
    Iterator<String> levels = Splitter.on(".").split(ipField).iterator();

    GenericRecord record = copyRecords(inputRecord, outputSchema, levels);

    return new SingleRecordIterable<>(record);
  }
}