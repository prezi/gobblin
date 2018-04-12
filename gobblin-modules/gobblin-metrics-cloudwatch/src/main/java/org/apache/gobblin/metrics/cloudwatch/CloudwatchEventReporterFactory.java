package org.apache.gobblin.metrics.cloudwatch;

import java.io.IOException;
import java.util.Properties;

import com.codahale.metrics.MetricRegistry;

import org.apache.gobblin.metrics.CustomCodahaleReporterFactory;
import org.apache.gobblin.metrics.MetricContext;


public class CloudwatchEventReporterFactory implements CustomCodahaleReporterFactory {

  @Override
  public com.codahale.metrics.ScheduledReporter newScheduledReporter(MetricRegistry registry, Properties properties)
      throws IOException {
    return CloudwatchEventReporter.Factory.forContext(MetricContext.class.cast(registry))
        .withCloudwatchPusher(new CloudwatchPusher())
        .build();
  }
}
