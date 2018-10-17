/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package org.apache.gobblin.mapreduce;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import org.apache.gobblin.compaction.dataset.Dataset;
import org.apache.gobblin.compaction.mapreduce.MRCompactorJobRunner;


/**
 * Created by tamasnemeth on 12/11/16.
 */
public class MrCompactorJsonKeyDedupJobRunner extends MRCompactorJobRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MrCompactorJsonKeyDedupJobRunner.class);

  public MrCompactorJsonKeyDedupJobRunner(Dataset dataset, FileSystem fs) {
    super(dataset, fs);
    this.dataset.jobProps().setProp("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
  }

  @Override
  protected void configureJob(Job job) throws IOException {
    super.configureJob(job);
    configureCompression(job);
  }

  private void configureCompression(Job job) {
    job.getConfiguration().setBoolean("mapreduce.output.fileoutputformat.compress", true);
    job.getConfiguration().set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
  }

  @Override
  protected void setInputFormatClass(Job job) {
    job.setInputFormatClass(TextInputFormat.class);
  }

  @Override
  protected void setMapperClass(Job job) {
    job.setMapperClass(JsonKeyMapper.class);
  }

  @Override
  protected void setMapOutputKeyClass(Job job) {
    job.setMapOutputKeyClass(Text.class);
  }

  @Override
  protected void setMapOutputValueClass(Job job) {
    job.setMapOutputValueClass(Text.class);
  }

  @Override
  protected void setOutputFormatClass(Job job) {
    job.setOutputFormatClass(JsonCompactorOutputFormat.class);
  }

  @Override
  protected void setReducerClass(Job job) {
    job.setReducerClass(JsonKeyDedupReducer.class);
  }

  @Override
  protected void setOutputKeyClass(Job job) {
    job.setOutputKeyClass(Text.class);
  }

  @Override
  protected void setOutputValueClass(Job job) {
    job.setOutputValueClass(NullWritable.class);
  }

  @Override
  protected Collection<String> getApplicableFileExtensions() {
    return Lists.newArrayList("gz");
  }


}
