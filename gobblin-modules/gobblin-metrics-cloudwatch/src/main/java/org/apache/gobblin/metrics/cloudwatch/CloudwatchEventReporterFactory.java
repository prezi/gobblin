package org.apache.gobblin.metrics.cloudwatch;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.CustomCodahaleReporterFactory;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.locks.JobLock;
import org.apache.gobblin.runtime.locks.JobLockException;
import org.apache.gobblin.runtime.locks.ZookeeperBasedJobLock;
import org.apache.gobblin.util.ConfigUtils;


public class CloudwatchEventReporterFactory implements CustomCodahaleReporterFactory {
  private static final String CLOUDWATCH_LOG_GROUP_NAME_CONFIG = "metrics.reporting.cloudwatch.log.group.name";
  private static final String CLOUDWATCH_LOG_STREAM_NAME_CONFIG = "metrics.reporting.cloudwatch.log.stream.name";
  private static final String CLOUDWATCH_IS_CREATE_LOG_STREAM = "metrics.reporting.cloudwatch.is.create.log.stream";

  private static final Logger LOGGER = LoggerFactory.getLogger(CloudwatchPusher.class);

  @Override
  public com.codahale.metrics.ScheduledReporter newScheduledReporter(MetricRegistry registry, Properties properties)
      throws IOException {
    Config config = ConfigUtils.propertiesToConfig(properties);

    JobLock lock;
    String logGroupName = ConfigUtils.emptyIfNotPresent(config, CLOUDWATCH_LOG_GROUP_NAME_CONFIG);
    String logStreamName = ConfigUtils.emptyIfNotPresent(config, CLOUDWATCH_LOG_STREAM_NAME_CONFIG);
    boolean isCreateLogStream = ConfigUtils.getBoolean(config, CLOUDWATCH_IS_CREATE_LOG_STREAM, false);

    LOGGER.info("Creating  CloudwatchEventReporter with LogGroupName: {}, LogStreamName: {}, isCreateLogStream: {}", logGroupName, logStreamName, isCreateLogStream);
    LOGGER.info("Properties file was: {}", properties);

    try {
      Properties propertiesNew = new Properties();
      propertiesNew.putAll(properties);
      propertiesNew.setProperty(ConfigurationKeys.JOB_NAME_KEY, "metrics"+"_"+properties.getProperty(ConfigurationKeys.JOB_NAME_KEY));
      lock = new ZookeeperBasedJobLock(propertiesNew);
    } catch (JobLockException e) {
      throw new RuntimeException(e);
    }

    return CloudwatchEventReporter.Factory.forContext(MetricContext.class.cast(registry))
        .withLogGroupName(logGroupName)
        .withLogStreamName(logStreamName)
        .withIsCreateLogStream(isCreateLogStream)
        .withLock(lock)
        .build();
  }
}
