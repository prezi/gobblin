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

package org.apache.gobblin.metrics.cloudwatch;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;


/**
 * Establishes a connection through the Graphite protocol and pushes timestamped name - value pairs
 *
 * @author Lorand Bendig
 *
 */
public class CloudwatchPusher implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(CloudwatchReporter.class);

  private LinkedBlockingQueue<MetricDatum> metrics = new LinkedBlockingQueue<>();

  private AmazonCloudWatch cloudwatchSender;

  public CloudwatchPusher() throws IOException {
    if (this.cloudwatchSender == null) {
      this.cloudwatchSender = AmazonCloudWatchClientBuilder.defaultClient();;
    }
  }

  /**
   * Pushes a single metrics through
   *
   * @param name metric name
   * @param value metric value
   * @param timestamp associated timestamp
   * @throws IOException
   */
  public void push(String name, Double value, long timestamp, List<Dimension> dimensions) throws IOException {
    MetricDatum datum = new MetricDatum()
        .withMetricName(name)
        .withUnit(StandardUnit.None)
        .withValue(value)
        .withTimestamp(new Date(timestamp))
        .withDimensions(dimensions);

    metrics.add(datum);

  }

  public void flush() throws IOException {
    LinkedList<MetricDatum> metricsToSend = new LinkedList<>();
    long numberOfRecords = metrics.drainTo(metricsToSend);

    if (numberOfRecords > 0 ) {
      PutMetricDataRequest request = new PutMetricDataRequest().withNamespace("prezi/data/gobblin").withMetricData(metricsToSend);

      this.cloudwatchSender.putMetricData(request);
      LOGGER.debug("{} metric was pushed to Cloudwatch",numberOfRecords);
    }else {
      LOGGER.debug("No metric to push to Cloudwatch");
    }
  }

  @Override
  public void close() throws IOException {
    this.cloudwatchSender.shutdown();
  }

}
