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

package org.apache.gobblin.metrics.graphite;

import java.io.IOException;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.cloudwatch.CloudwatchEventReporter;
import org.apache.gobblin.metrics.cloudwatch.CloudwatchPusher;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.JobEvent;
import org.apache.gobblin.metrics.event.MultiPartEvent;
import org.apache.gobblin.metrics.event.TaskEvent;


/**
 * Test for GraphiteEventReporter using a mock backend ({@link TestGraphiteSender})
 *
 * @author Lorand Bendig
 *
 */
@Test(groups = { "gobblin.metrics" })
public class CloudwatchEventReporterTest {

  private static int DEFAULT_PORT = 0;
  private static String DEFAULT_HOST = "localhost";
  private static String NAMESPACE = "gobblin.metrics.test";

  //private TestGraphiteSender cloudwatchSender = new TestCloudwatchSender();
  private CloudwatchPusher cloudwatchPusher;

  @BeforeClass
  public void setUp() throws IOException {
    this.cloudwatchPusher = new CloudwatchPusher();
  }

  private CloudwatchEventReporter.BuilderImpl getBuilder(MetricContext metricContext) {
    return CloudwatchEventReporter.Factory.forContext(metricContext).withCloudwatchPusher(cloudwatchPusher);
  }

  @Test
  public void testSimpleEvent() throws IOException {
    try (
        MetricContext metricContext =
            MetricContext.builder(this.getClass().getCanonicalName() + ".testCloudwatchReporter1").build();

        CloudwatchEventReporter cloudwatchEventReporter = getBuilder(metricContext).withEmitValueAsKey(false).build();) {

      Map<String, String> metadata = Maps.newHashMap();
      metadata.put(JobEvent.METADATA_JOB_ID, "job1");
      metadata.put(TaskEvent.METADATA_TASK_ID, "task1");

      metricContext.submitEvent(GobblinTrackingEvent.newBuilder()
          .setName(JobEvent.TASKS_SUBMITTED)
          .setNamespace(NAMESPACE)
          .setMetadata(metadata).build());

      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }

      cloudwatchEventReporter.report();

      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }

     // TimestampedValue retrievedEvent = cloudwatchSender.getMetric("gobblin.metrics.job1.task1.events.TasksSubmitted");

     // Assert.assertEquals(retrievedEvent.getValue(), "0");
     // Assert.assertTrue(retrievedEvent.getTimestamp() <= (System.currentTimeMillis() / 1000l));

    }
  }

  @Test
  public void testMultiPartEvent() throws IOException {
    try (
        MetricContext metricContext =
            MetricContext.builder(this.getClass().getCanonicalName() + ".testCloudwatchReporter2").build();

        CloudwatchEventReporter graphiteEventReporter = getBuilder(metricContext).withEmitValueAsKey(true).build();) {

      Map<String, String> metadata = Maps.newHashMap();
      metadata.put(JobEvent.METADATA_JOB_ID, "job2");
      metadata.put(TaskEvent.METADATA_TASK_ID, "task2");
      metadata.put(EventSubmitter.EVENT_TYPE, "JobStateEvent");
      metadata.put(JobEvent.METADATA_JOB_START_TIME, "1457736710521");
      metadata.put(JobEvent.METADATA_JOB_END_TIME, "1457736710734");
      metadata.put(JobEvent.METADATA_JOB_LAUNCHED_TASKS, "3");
      metadata.put(JobEvent.METADATA_JOB_COMPLETED_TASKS, "2");
      metadata.put(JobEvent.METADATA_JOB_STATE, "FAILED");

      metricContext.submitEvent(GobblinTrackingEvent.newBuilder()
          .setName(MultiPartEvent.JOBSTATE_EVENT.getEventName())
          .setNamespace(NAMESPACE)
          .setMetadata(metadata).build());

      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }

      graphiteEventReporter.report();

      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }

      String prefix = "gobblin.metrics.job2.task2.events.JobStateEvent";
/*
      Assert.assertEquals(cloudwatchSender.getMetric(prefix + ".jobBeginTime").getValue(), "1457736710521");
      Assert.assertEquals(cloudwatchSender.getMetric(prefix + ".jobEndTime").getValue(), "1457736710734");
      Assert.assertEquals(cloudwatchSender.getMetric(prefix + ".jobLaunchedTasks").getValue(), "3");
      Assert.assertEquals(cloudwatchSender.getMetric(prefix + ".jobCompletedTasks").getValue(), "2");
      Assert.assertNotNull(cloudwatchSender.getMetric(prefix + ".jobState.FAILED"));
*/
    }
  }
}
