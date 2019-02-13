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

package org.apache.gobblin.hive.avro;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.codahale.metrics.Timer;
import com.github.rholder.retry.Retryer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveSerDeManager;
import org.apache.gobblin.hive.HiveSerDeWrapper;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.retry.RetryerFactory;

import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_INTERVAL_MS;
import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_MULTIPLIER;
import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TIME_OUT_MS;
import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TYPE;

import static org.apache.gobblin.hive.policy.PartitionAwareHiveRegistrationPolicy.SCHEMA_SET;


/**
 * A {@link HiveSerDeManager} for registering Avro tables and partitions.
 *
 * @author Ziyang Liu
 */
@Slf4j
@Alpha
public class HiveAvroSerDeManager extends HiveSerDeManager {

  public static final String SCHEMA_LITERAL = "avro.schema.literal";
  public static final String SCHEMA_URL = "avro.schema.url";
  public static final String USE_SCHEMA_FILE = "use.schema.file";
  public static final boolean DEFAULT_USE_SCHEMA_FILE = false;
  public static final String SCHEMA_FILE_NAME = "schema.file.name";
  public static final String DEFAULT_SCHEMA_FILE_NAME = "_schema.avsc";
  public static final String SCHEMA_LITERAL_LENGTH_LIMIT = "schema.literal.length.limit";
  public static final int DEFAULT_SCHEMA_LITERAL_LENGTH_LIMIT = 4000;
  public static final String HIVE_SPEC_SCHEMA_READING_TIMER = "hiveAvroSerdeManager.schemaReadTimer";
  public static final String HIVE_SPEC_SCHEMA_WRITING_TIMER = "hiveAvroSerdeManager.schemaWriteTimer";

  protected final FileSystem fs;
  protected final boolean useSchemaFile;
  protected final boolean setSchema;
  protected final String schemaFileName;
  protected final int schemaLiteralLengthLimit;
  protected final HiveSerDeWrapper serDeWrapper = HiveSerDeWrapper.get("AVRO");

  private final MetricContext metricContext ;

  static final Config COMPACTION_RETRY_DEFAULTS;

  static {
    Map<String, Object> configMap =
        ImmutableMap.<String, Object>builder()
            .put(RETRY_TIME_OUT_MS, TimeUnit.MINUTES.toMillis(2L))   //Overall retry for 2 minutes
            .put(RETRY_INTERVAL_MS, TimeUnit.SECONDS.toMillis(5L)) //Try to retry 5 seconds
            .put(RETRY_MULTIPLIER, 2L) // Muliply by 2 every attempt
            .put(RETRY_TYPE, RetryerFactory.RetryType.EXPONENTIAL.name())
            .build();
    COMPACTION_RETRY_DEFAULTS = ConfigFactory.parseMap(configMap);
  };

  public HiveAvroSerDeManager(State props) throws IOException {
    super(props);

    if (props.contains(HiveRegistrationPolicyBase.HIVE_FS_URI)) {
      this.fs = FileSystem.get(URI.create(props.getProp(HiveRegistrationPolicyBase.HIVE_FS_URI)), HadoopUtils.getConfFromState(props));
    } else {
      this.fs = FileSystem.get(HadoopUtils.getConfFromState(props));
    }

    this.setSchema = props.getPropAsBoolean(SCHEMA_SET, true);
    this.useSchemaFile = props.getPropAsBoolean(USE_SCHEMA_FILE, DEFAULT_USE_SCHEMA_FILE);
    this.schemaFileName = props.getProp(SCHEMA_FILE_NAME, DEFAULT_SCHEMA_FILE_NAME);
    this.schemaLiteralLengthLimit =
        props.getPropAsInt(SCHEMA_LITERAL_LENGTH_LIMIT, DEFAULT_SCHEMA_LITERAL_LENGTH_LIMIT);

    this.metricContext = Instrumented.getMetricContext(props, HiveAvroSerDeManager.class);
    }

  /**
   * Add an Avro {@link Schema} to the given {@link HiveRegistrationUnit}.
   *
   *  <p>
   *    If {@link #USE_SCHEMA_FILE} is true, the schema will be added via {@link #SCHEMA_URL} pointing to
   *    the schema file named {@link #SCHEMA_FILE_NAME}.
   *  </p>
   *
   *  <p>
   *    If {@link #USE_SCHEMA_FILE} is false, the schema will be obtained by {@link #getDirectorySchema(Path)}.
   *    If the length of the schema is less than {@link #SCHEMA_LITERAL_LENGTH_LIMIT}, it will be added via
   *    {@link #SCHEMA_LITERAL}. Otherwise, the schema will be written to {@link #SCHEMA_FILE_NAME} and added
   *    via {@link #SCHEMA_URL}.
   *  </p>
   */
  @Override
  public void addSerDeProperties(Path path, HiveRegistrationUnit hiveUnit) throws IOException {
    hiveUnit.setSerDeType(this.serDeWrapper.getSerDe().getClass().getName());
    hiveUnit.setInputFormat(this.serDeWrapper.getInputFormatClassName());
    hiveUnit.setOutputFormat(this.serDeWrapper.getOutputFormatClassName());

    if (setSchema) {
      addSchemaProperties(path, hiveUnit);
    }
  }

  @Override
  public void addSerDeProperties(HiveRegistrationUnit source, HiveRegistrationUnit target) throws IOException {
    if (source.getSerDeType().isPresent()) {
      target.setSerDeType(source.getSerDeType().get());
    }
    if (source.getInputFormat().isPresent()) {
      target.setInputFormat(source.getInputFormat().get());
    }
    if (source.getOutputFormat().isPresent()) {
      target.setOutputFormat(source.getOutputFormat().get());
    }

    if (setSchema) {
      if (source.getSerDeProps().contains(SCHEMA_LITERAL)) {
        target.setSerDeProp(SCHEMA_LITERAL, source.getSerDeProps().getProp(SCHEMA_LITERAL));
      }
      if (source.getSerDeProps().contains(SCHEMA_URL)) {
        target.setSerDeProp(SCHEMA_URL, source.getSerDeProps().getProp(SCHEMA_URL));
      }
    }
  }

  private void addSchemaProperties(Path path, HiveRegistrationUnit hiveUnit) throws IOException {
    Preconditions.checkArgument(this.fs.getFileStatus(path).isDirectory(), path + " is not a directory.");

    Path schemaFile = new Path(path, this.schemaFileName);
    if (this.useSchemaFile) {
      hiveUnit.setSerDeProp(SCHEMA_URL, schemaFile.toString());
    } else {
      Schema schema ;
      try (Timer.Context context = metricContext.timer(HIVE_SPEC_SCHEMA_READING_TIMER).time()) {
        schema = getDirectorySchema(path);
      }
      try (Timer.Context context = metricContext.timer(HIVE_SPEC_SCHEMA_WRITING_TIMER).time()) {
        addSchemaFromAvroFile(schema, schemaFile, hiveUnit);
      }
    }
  }

  /**
   * Get schema for a directory using {@link AvroUtils#getDirectorySchema(Path, FileSystem, boolean)}.
   */
  protected Schema getDirectorySchema(Path directory) throws IOException {
    Retryer<Schema> retryer = RetryerFactory.newInstance(COMPACTION_RETRY_DEFAULTS);
    Schema directorySchema = null;

    try {
      directorySchema = retryer.call(() -> {
        log.info("Getting directory schema from path {}", directory);
        Schema schema = AvroUtils.getDirectorySchema(directory, this.fs, true);
        return schema;
      });
    } catch (Exception e) {
      throw new IOException("Unable to get schema from path "+ directory +" and won't retry anymore." + e);
    }
    return directorySchema;
  }

  /**
   * Add a {@link Schema} obtained from an Avro data file to the given {@link HiveRegistrationUnit}.
   *
   *  <p>
   *    If the length of the schema is less than {@link #SCHEMA_LITERAL_LENGTH_LIMIT}, it will be added via
   *    {@link #SCHEMA_LITERAL}. Otherwise, the schema will be written to {@link #SCHEMA_FILE_NAME} and added
   *    via {@link #SCHEMA_URL}.
   *  </p>
   */
  protected void addSchemaFromAvroFile(Schema schema, Path schemaFile, HiveRegistrationUnit hiveUnit)
      throws IOException {
    Preconditions.checkNotNull(schema);
    Retryer<Void> retryer = RetryerFactory.newInstance(COMPACTION_RETRY_DEFAULTS);

    String schemaStr = schema.toString();
    if (schemaStr.length() <= this.schemaLiteralLengthLimit) {
      log.info("Not using Avro file as literal is smaller than limit {}", this.schemaLiteralLengthLimit);
      hiveUnit.setSerDeProp(SCHEMA_LITERAL, schema.toString());
    } else {
        try {
          log.info("Using schema file for {}", schema.toString());

          retryer.call(() -> {
            log.info("Writing schema file to {}", schemaFile.getName());
            AvroUtils.writeSchemaToFile(schema, schemaFile, this.fs, true);
            if (!fs.exists(schemaFile)) {
              log.info("Schema file {} does not exists", schemaFile.getName());
              throw new IOException("Schema file " + schemaFile + " does not exists however it should. Will wait more.");
            }
            return null;
          });
        } catch (Exception e) {
          throw new IOException(e);
        }

      log.info("Using schema file " + schemaFile.toString());
      hiveUnit.setSerDeProp(SCHEMA_URL, schemaFile.toString());
    }
  }

  @Override
  public void updateSchema(HiveRegistrationUnit existingUnit, HiveRegistrationUnit newUnit) throws IOException {
    Preconditions.checkArgument(
        newUnit.getSerDeProps().contains(SCHEMA_LITERAL) || newUnit.getSerDeProps().contains(SCHEMA_URL));

    if (newUnit.getSerDeProps().contains(SCHEMA_LITERAL)) {
      existingUnit.setSerDeProp(SCHEMA_LITERAL, newUnit.getSerDeProps().getProp(SCHEMA_LITERAL));
    } else {
      existingUnit.setSerDeProp(SCHEMA_URL, newUnit.getSerDeProps().getProp(SCHEMA_URL));
    }
  }

  @Override
  public boolean haveSameSchema(HiveRegistrationUnit unit1, HiveRegistrationUnit unit2) {
    if (unit1.getSerDeProps().contains(HiveAvroSerDeManager.SCHEMA_LITERAL)
        && unit2.getSerDeProps().contains(HiveAvroSerDeManager.SCHEMA_LITERAL)) {
      return unit1.getSerDeProps().getProp(HiveAvroSerDeManager.SCHEMA_LITERAL)
          .equals(unit2.getSerDeProps().getProp(HiveAvroSerDeManager.SCHEMA_LITERAL));
    } else if (unit1.getSerDeProps().contains(HiveAvroSerDeManager.SCHEMA_URL)
        && unit2.getSerDeProps().contains(HiveAvroSerDeManager.SCHEMA_URL)) {
      return unit1.getSerDeProps().getProp(HiveAvroSerDeManager.SCHEMA_URL)
          .equals(unit2.getSerDeProps().getProp(HiveAvroSerDeManager.SCHEMA_URL));
    }
    return false;
  }

}
