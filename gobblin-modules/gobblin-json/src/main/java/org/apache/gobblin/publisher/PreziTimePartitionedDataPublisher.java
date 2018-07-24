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

package org.apache.gobblin.publisher;

import java.io.IOException;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.publisher.TimePartitionedDataPublisher;
import org.apache.gobblin.util.WriterUtils;

import static org.apache.gobblin.configuration.ConfigurationKeys.DATA_PUBLISHER_PREFIX;


/**
 * For time partition jobs, writer output directory is
 * $GOBBLIN_WORK_DIR/task-output/{extractId}/{tableName}/{partitionPath},
 * where partition path is the time bucket, e.g., 2015/04/08/15.
 *
 * Publisher output directory is $GOBBLIN_WORK_DIR/job-output/{tableName}/{partitionPath}
 *
 * If you set the {@link DATA_PUBLISHER_PREFIX}.removefrompath then the specified prefix will be removed.
 * Like finance.log.payment can be stripped to payment with
 * {@link DATA_PUBLISHER_PREFIX}.removefrompath = "finance.log."
 */
public class PreziTimePartitionedDataPublisher extends TimePartitionedDataPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(PreziTimePartitionedDataPublisher.class);

  protected final String prefixToRemove;


  public PreziTimePartitionedDataPublisher(State state) throws IOException {
    super(state);

    this.prefixToRemove = this.getState().getProp(DATA_PUBLISHER_PREFIX + ".removefrompath", "");

  }

  @Override
  protected Path getPublisherOutputDir(WorkUnitState workUnitState, int branchId) {
    Path publisherOutput = WriterUtils.getDataPublisherFinalDir(workUnitState, this.numBranches, branchId);

    if (!this.prefixToRemove.isEmpty()) {
      String escapedPrefix = StringEscapeUtils.escapeJava(prefixToRemove);
      publisherOutput = new Path(publisherOutput.toString().replaceAll(escapedPrefix, ""));
      LOG.info("Modified path: "+publisherOutput);
    }
    return publisherOutput;
  }

}
