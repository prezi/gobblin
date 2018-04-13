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

import java.io.IOException;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.reporter.EventReporter;
import org.apache.gobblin.metrics.reporter.util.AvroJsonSerializer;
import org.apache.gobblin.metrics.reporter.util.AvroSerializer;
import org.apache.gobblin.metrics.reporter.util.NoopSchemaVersionWriter;
import org.apache.gobblin.metrics.reporter.util.SchemaVersionWriter;


/**
 *
 * {@link org.apache.gobblin.metrics.reporter.EventReporter} that emits {@link org.apache.gobblin.metrics.GobblinTrackingEvent} events
 * as timestamped name - value pairs through the Graphite protocol
 *
 * @author Lorand Bendig
 *
 */
public class CloudwatchEventReporter extends EventReporter {

  private static final String DEFAULT_LOG_GROUP_NAME = "gobblin";
  private final CloudwatchPusher cloudwatchPusher;

  private static final Logger LOGGER = LoggerFactory.getLogger(CloudwatchEventReporter.class);
  private final AvroSerializer<GobblinTrackingEvent> serializer;

  public CloudwatchEventReporter(Builder<?> builder) throws IOException {
    super(builder);
    if (builder.cloudwatchPusher.isPresent()) {
      this.cloudwatchPusher = builder.cloudwatchPusher.get();
    } else {
      this.cloudwatchPusher =
          this.closer.register(new CloudwatchPusher(builder.logGroupName, builder.logStreamName, builder.isCreateLogStream));
    }
    this.serializer = this.closer.register(
        createSerializer(new NoopSchemaVersionWriter()));

  }

  protected AvroSerializer<GobblinTrackingEvent> createSerializer(SchemaVersionWriter schemaVersionWriter) throws IOException {
    return new AvroJsonSerializer<GobblinTrackingEvent>(GobblinTrackingEvent.SCHEMA$, schemaVersionWriter);
  }

  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {

    GobblinTrackingEvent nextEvent;
    try {
      while (null != (nextEvent = queue.poll())) {
        pushEvent(nextEvent);
      }
      this.cloudwatchPusher.flush();
    } catch (IOException e) {
      LOGGER.error("Error sending event to Cloudwatch", e);
      try {
        this.cloudwatchPusher.flush();
      } catch (IOException e1) {
        LOGGER.error("Unable to flush previous events to Cloudwatch", e);
      }
    }
  }

  /**
   * Extracts the event and its metadata from {@link GobblinTrackingEvent} and creates
   * timestamped name value pairs
   *
   * @param event {@link GobblinTrackingEvent} to be reported
   * @throws IOException
   */
  private void pushEvent(GobblinTrackingEvent event) throws IOException {
    String jsonEvent =
        new String(this.serializer.serializeRecord(event), "UTF8").trim();
    cloudwatchPusher.push(event.getTimestamp(), event.getMetadata().get(EventSubmitter.EVENT_TYPE), jsonEvent);
  }

  /**
   * Returns a new {@link CloudwatchEventReporter.Builder} for {@link CloudwatchEventReporter}.
   * Will automatically add all Context tags to the reporter.
   *
   * @param context the {@link org.apache.gobblin.metrics.MetricContext} to report
   * @return GraphiteEventReporter builder
   * @deprecated this method is bugged. Use {@link CloudwatchEventReporter.Factory#forContext} instead.
   */
  @Deprecated
  public static Builder<? extends Builder> forContext(MetricContext context) {
    return new BuilderImpl(context);
  }

  public static class BuilderImpl extends Builder<BuilderImpl> {

    private BuilderImpl(MetricContext context) {
      super(context);
    }

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  public static class Factory {
    /**
     * Returns a new {@link CloudwatchEventReporter.Builder} for {@link CloudwatchEventReporter}.
     * Will automatically add all Context tags to the reporter.
     *
     * @param context the {@link org.apache.gobblin.metrics.MetricContext} to report
     * @return GraphiteEventReporter builder
     */
    public static BuilderImpl forContext(MetricContext context) {
      return new BuilderImpl(context);
    }
  }

  /**
   * Builder for {@link CloudwatchEventReporter}.
   * Defaults to no filter
   */
  public static abstract class Builder<T extends EventReporter.Builder<T>> extends EventReporter.Builder<T> {
    protected Optional<CloudwatchPusher> cloudwatchPusher;
    private String logGroupName = DEFAULT_LOG_GROUP_NAME;
    private String logStreamName;
    private boolean isCreateLogStream = false;

    protected Builder(MetricContext context) {
      super(context);
      this.cloudwatchPusher = Optional.absent();
    }

    /**
     * Set {@link org.apache.gobblin.metrics.cloudwatch.CloudwatchPusher} to use.
     */
    public T withCloudwatchPusher(CloudwatchPusher pusher) {
      this.cloudwatchPusher = Optional.of(pusher);
      return self();
    }


    public T withLogGroupName(String logGroupName) {
      this.logGroupName = logGroupName;
      return self();
    }

    public T withLogStreamName(String logStreamName) {
      this.logStreamName = logStreamName;
      return self();
    }

    public T withIsCreateLogStream(boolean isCreateLogStream) {
      this.isCreateLogStream = isCreateLogStream;
      return self();
    }

    /**
     * Builds and returns {@link CloudwatchEventReporter}.
     *
     * @return GraphiteEventReporter
     */
    public CloudwatchEventReporter build() throws IOException {
      return new CloudwatchEventReporter(this);
    }
  }

}
