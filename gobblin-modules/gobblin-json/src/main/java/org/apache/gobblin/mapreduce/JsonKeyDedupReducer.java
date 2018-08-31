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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reducer job for json files
 * Created by tamasnemeth on 12/11/16.
 */
class JsonKeyDedupReducer extends Reducer<Text, Text, Text, NullWritable>{
  private static final Logger LOG = LoggerFactory.getLogger(JsonKeyDedupReducer.class);

  public enum EVENT_COUNTER {
    MORE_THAN_1,
    DEDUPED,
    RECORD_COUNT
  }

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    long numVals = 0;

    Text valueToRetain = null;

    for (Text value : values) {
      if (valueToRetain == null) {
        valueToRetain = value;
      }

      numVals++;
    }

    if (numVals > 1) {
      context.getCounter(JsonKeyDedupReducer.EVENT_COUNTER.MORE_THAN_1).increment(1);
      context.getCounter(JsonKeyDedupReducer.EVENT_COUNTER.DEDUPED).increment(numVals - 1);
    }

    context.getCounter(JsonKeyDedupReducer.EVENT_COUNTER.RECORD_COUNT).increment(1);

    context.write(valueToRetain, NullWritable.get());
  }

}
