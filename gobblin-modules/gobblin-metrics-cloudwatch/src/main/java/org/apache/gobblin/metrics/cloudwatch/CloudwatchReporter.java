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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;

import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.Measurements;
import org.apache.gobblin.metrics.reporter.ConfiguredScheduledReporter;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.metrics.Measurements.*;

public class CloudwatchReporter extends ConfiguredScheduledReporter {

  private final CloudwatchPusher cloudwatchPusher;

  private static final Logger LOGGER = LoggerFactory.getLogger(CloudwatchReporter.class);

  public CloudwatchReporter(Builder<?> builder, Config config) throws IOException {
    super(builder, config);
    if (builder.cloudwatchPusher.isPresent()) {
      this.cloudwatchPusher = builder.cloudwatchPusher.get();
    } else {
      this.cloudwatchPusher = this.closer.register(new CloudwatchPusher());
    }
  }

  /**
   * A static factory class for obtaining new {@link org.apache.gobblin.metrics.cloudwatch.CloudwatchReporter.Builder}s
   *
   * @see org.apache.gobblin.metrics.cloudwatch.CloudwatchReporter.Builder
   */
  public static class Factory {

    public static BuilderImpl newBuilder() {
      return new BuilderImpl();
    }
  }

  public static class BuilderImpl extends Builder<BuilderImpl> {

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  /**
   * Builder for {@link CloudwatchReporter}. Defaults to no filter, reporting rates in seconds and times in
   * milliseconds
   */
  public static abstract class Builder<T extends ConfiguredScheduledReporter.Builder<T>> extends
      ConfiguredScheduledReporter.Builder<T> {

    protected MetricFilter filter;
    protected String hostname;
    protected int port;
    protected Optional<CloudwatchPusher> cloudwatchPusher;

    protected Builder() {
      super();
      this.name = "CloudwatchReporter";
      this.cloudwatchPusher = Optional.absent();
      this.filter = MetricFilter.ALL;
    }

    /**
     * Set {@link org.apache.gobblin.metrics.cloudwatch.CloudwatchPusher} to use.
     */
    public T withGraphitePusher(CloudwatchPusher pusher) {
      this.cloudwatchPusher = Optional.of(pusher);
      return self();
    }

    /**
     * Set connection parameters for the {@link org.apache.gobblin.metrics.cloudwatch.CloudwatchPusher} creation
     */
    public T withConnection(String hostname, int port) {
      this.hostname = hostname;
      this.port = port;
      return self();
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public T filter(MetricFilter filter) {
      this.filter = filter;
      return self();
    }

    /**
     * Builds and returns {@link CloudwatchReporter}.
     *
     * @param props metrics properties
     * @return GraphiteReporter
     */
    public CloudwatchReporter build(Properties props) throws IOException {
      return new CloudwatchReporter(this, ConfigUtils.propertiesToConfig(props,
          Optional.of(ConfigurationKeys.METRICS_CONFIGURATIONS_PREFIX)));
    }
  }

  @Override
  protected void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers,
      Map<String, Object> tags) {

    String prefix = getMetricNamePrefix(tags);
    List<Dimension> dimensions =  createDimensionsFromTags(tags);
    long timestamp = System.currentTimeMillis();

    try {
      for (Map.Entry<String, Gauge> gauge : gauges.entrySet()) {
        reportGauge(prefix, gauge.getKey(), gauge.getValue(), timestamp, dimensions);
      }

      for (Map.Entry<String, Counter> counter : counters.entrySet()) {
        reportCounter(prefix, counter.getKey(), counter.getValue(), timestamp, dimensions);
      }

      for (Map.Entry<String, Histogram> histogram : histograms.entrySet()) {
        reportHistogram(prefix, histogram.getKey(), histogram.getValue(), timestamp, dimensions);
      }

      for (Map.Entry<String, Meter> meter : meters.entrySet()) {
        reportMetered(prefix, meter.getKey(), meter.getValue(), timestamp, dimensions);
      }

      for (Map.Entry<String, Timer> timer : timers.entrySet()) {
        reportTimer(prefix, timer.getKey(), timer.getValue(), timestamp, dimensions);
      }

      this.cloudwatchPusher.flush();

    } catch (IOException ioe) {
      LOGGER.error("Error sending metrics to Cloudwatch", ioe);
      try {
        this.cloudwatchPusher.close();
      } catch (IOException innerIoe) {
        LOGGER.error("Error closing the Cloudwatch sender", innerIoe);
      }
    }
  }

  private List<Dimension> createDimensionsFromTags(Map<String, Object> tags) {
    LinkedList<Dimension> dimensions = new LinkedList<>();

    for (Map.Entry<String, Object> entry : tags.entrySet()) {
      Dimension dimension = new Dimension();
      dimension.setName(entry.getKey());
      dimension.setValue(entry.getValue().toString());

      dimensions.add(dimension);
    }

    return dimensions;
  }

  private void reportGauge(String prefix, String name, Gauge gauge, long timestamp, List<Dimension> dimensions) throws IOException {
    String metricName = getKey(prefix, name);
    Object value = gauge.getValue();
    Double metricValue;
    try {
      metricValue = NumberUtils.createDouble(value.toString());

      if (metricValue == null) {
        return;
      }
    } catch (NumberFormatException e){
      return;
    }

    pushMetric(metricName, metricValue, timestamp, dimensions);
  }

  private void reportCounter(String prefix, String name, Counting counter, long timestamp, List<Dimension> dimensions) throws IOException {
    String metricName = getKey(prefix, name, COUNT.getName());
    pushMetric(metricName, counter.getCount(), false, timestamp, dimensions);
  }

  private void reportHistogram(String prefix, String name, Histogram histogram, long timestamp, List<Dimension> dimensions) throws IOException {
    reportCounter(prefix, name, histogram, timestamp, dimensions);
    reportSnapshot(prefix, name, histogram.getSnapshot(), timestamp, dimensions, false);
  }

  private void reportTimer(String prefix, String name, Timer timer, long timestamp, List<Dimension> dimensions) throws IOException {
    reportSnapshot(prefix, name, timer.getSnapshot(), timestamp, dimensions, true);
    reportMetered(prefix, name, timer, timestamp, dimensions);
  }

  private void reportSnapshot(String prefix, String name, Snapshot snapshot, long timestamp, List<Dimension> dimensions, boolean convertDuration)
      throws IOException {
    String baseMetricName = getKey(prefix, name);
    pushMetric(getKey(baseMetricName, MIN), snapshot.getMin(), convertDuration, timestamp, dimensions);
    pushMetric(getKey(baseMetricName, MAX), snapshot.getMax(), convertDuration, timestamp, dimensions);
    pushMetric(getKey(baseMetricName, MEAN), snapshot.getMean(), convertDuration, timestamp, dimensions);
    pushMetric(getKey(baseMetricName, STDDEV), snapshot.getStdDev(), convertDuration, timestamp, dimensions);
    pushMetric(getKey(baseMetricName, MEDIAN), snapshot.getMedian(), convertDuration, timestamp, dimensions);
    pushMetric(getKey(baseMetricName, PERCENTILE_75TH), snapshot.get75thPercentile(), convertDuration, timestamp, dimensions);
    pushMetric(getKey(baseMetricName, PERCENTILE_95TH), snapshot.get95thPercentile(), convertDuration, timestamp, dimensions);
    pushMetric(getKey(baseMetricName, PERCENTILE_98TH), snapshot.get98thPercentile(), convertDuration, timestamp, dimensions);
    pushMetric(getKey(baseMetricName, PERCENTILE_99TH), snapshot.get99thPercentile(), convertDuration, timestamp, dimensions);
    pushMetric(getKey(baseMetricName, PERCENTILE_999TH), snapshot.get999thPercentile(), convertDuration, timestamp, dimensions);
  }

  private void reportMetered(String prefix, String name, Metered metered, long timestamp, List<Dimension> dimensions) throws IOException {
    reportCounter(prefix, name, metered, timestamp, dimensions);
    String baseMetricName = getKey(prefix, name);
    pushMetricRate(getKey(baseMetricName, RATE_1MIN), metered.getOneMinuteRate(), timestamp, dimensions);
    pushMetricRate(getKey(baseMetricName, RATE_5MIN), metered.getFiveMinuteRate(), timestamp, dimensions);
    pushMetricRate(getKey(baseMetricName, RATE_15MIN), metered.getFifteenMinuteRate(), timestamp, dimensions);
    pushMetricRate(getKey(baseMetricName, MEAN_RATE), metered.getMeanRate(), timestamp, dimensions);
  }

  private void pushMetric(String metricName, Number value, boolean toDuration, long timestamp, List<Dimension> dimensions) throws IOException {
    Double metricValue = toDuration ? convertDuration(value.doubleValue()) : value.doubleValue();
    pushMetric(metricName, metricValue, timestamp, dimensions);
  }

  private void pushMetricRate(String metricName, double value, long timestamp, List<Dimension> dimensions)
      throws IOException {
    pushMetric(metricName, convertRate(value), timestamp, dimensions);
  }

  private void pushMetric(String name, Double value, long timestamp, List<Dimension> dimensions) throws IOException {
    this.cloudwatchPusher.push(name, value, timestamp, dimensions);
  }

  private String getKey(String baseName, Measurements measurements) {
    return getKey(baseName, measurements.getName());
  }

  private String getKey(String... keys) {
    return JOINER.join(keys);
  }

}
