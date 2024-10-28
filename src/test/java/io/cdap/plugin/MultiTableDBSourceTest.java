/*
 * Copyright © 2024 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin;


import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.plugin.format.MultiTableConf;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for MultiTableDBSource
 */
public class MultiTableDBSourceTest {

  MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(Schema.of(Schema.Type.STRING));
  MultiTableDBSource multiTableDBSource;
  String referenceNameWithSpace = "Hello MultiTable";

  @Before
  public void setUp() {
    multiTableDBSource = new MultiTableDBSource(new MultiTableConf(referenceNameWithSpace));
  }

  /**
   * Plugin should not allow space in reference name for MultiTableDBSource
   */
  @Test
  public void testMultiTableReferenceNameWithSpace() {
    try {
      multiTableDBSource.configurePipeline(mockPipelineConfigurer);
    } catch (ValidationException e) {
      // expected
    }
    Assert.assertEquals(1, mockPipelineConfigurer.getStageConfigurer().getFailureCollector().getValidationFailures()
      .size());
  }
}
