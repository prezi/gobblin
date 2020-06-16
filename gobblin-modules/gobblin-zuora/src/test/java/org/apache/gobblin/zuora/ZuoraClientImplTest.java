package org.apache.gobblin.zuora;

import java.util.List;

import org.testng.Assert;

import avro.shaded.com.google.common.collect.Lists;
import gobblin.configuration.WorkUnitState;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.source.extractor.extract.Command;


public class ZuoraClientImplTest {

  @org.testng.annotations.Test
  public void testBuildPostCommand() {
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME, "test_host");
    workUnitState.setProp(ZuoraConfigurationKeys.ZUORA_API_RETRY_POST_COUNT, 5);
    workUnitState.setProp(ConfigurationKeys.JOB_NAME_KEY, "zuora-test-job");
    workUnitState.setProp(ZuoraConfigurationKeys.ZUORA_DELETED_COLUMN, "is_deleted");
    ZuoraClientImpl client = new ZuoraClientImpl(workUnitState);
    List<Command> ret = client.buildPostCommand(Lists.newArrayList());
    Assert.assertEquals(ret.size(), 0);

  }
}
