/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.plugin.cobol;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class CobolRecordConverterConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Schema VALID_SCHEMA =
    Schema.recordOf("schema",
                    Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));

  private static final String copybook = "       01  CUSTOMER-DATA.\n" +
    "           05 CUSTOMER-ID                    PIC 9(6).\n" +
    "           05 PERSONAL-DATA.\n" +
    "              10 CUSTOMER-NAME               PIC X(20).\n" +
    "              10 CUSTOMER-ADDRESS            PIC X(20).\n" +
    "              10 CUSTOMER-PHONE              PIC X(8).\n" +
    "           05 TRANSACTIONS.\n" +
    "              10 TRANSACTION-NBR             PIC 9(9) COMP.\n" +
    "              10 TRANSACTION OCCURS 0 TO 5\n" +
    "                 DEPENDING ON TRANSACTION-NBR.\n" +
    "                 15 TRANSACTION-DATE         PIC X(8).\n" +
    "                 15 FILLER REDEFINES TRANSACTION-DATE.\n" +
    "                    20 TRANSACTION-DAY       PIC X(2).\n" +
    "                    20 FILLER                PIC X.\n" +
    "                    20 TRANSACTION-MONTH     PIC X(2).\n" +
    "                    20 FILLER                PIC X.\n" +
    "                    20 TRANSACTION-YEAR      PIC X(2).\n" +
    "                 15 TRANSACTION-AMOUNT       PIC S9(13)V99 COMP-3.\n" +
    "                 15 TRANSACTION-COMMENT      PIC X(9).";

  private static final CobolRecordConverterConfig VALID_CONFIG =
    new CobolRecordConverterConfig(copybook, "FIXED_FORMAT",
                                   "IBM01140", true, "body");

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.getOutputSchemaAndValidate(failureCollector, VALID_SCHEMA);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testContentFieldDoesNotExists() {
    Schema schema = Schema.recordOf("schema",
                      Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.getOutputSchemaAndValidate(failureCollector, schema);
    assertValidationFailed(failureCollector, CobolRecordConverterConfig.PROPERTY_CONTENT_FIELD_NAME);
  }

  @Test
  public void testContentFieldOfWrongType() {
    CobolRecordConverterConfig config = CobolRecordConverterConfig.builder(VALID_CONFIG)
      .setContentFieldName("nonExisting")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.getOutputSchemaAndValidate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, CobolRecordConverterConfig.PROPERTY_CONTENT_FIELD_NAME);
  }

  @Test
  public void testInvalidCharset() {
    CobolRecordConverterConfig config = CobolRecordConverterConfig.builder(VALID_CONFIG)
      .setCharset("nonExisting")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    try {
      config.getOutputSchemaAndValidate(failureCollector, VALID_SCHEMA);
      Assert.fail("ValidationException exception was expected, but not thrown.");
    } catch (ValidationException ex) {
    }

    assertValidationFailed(failureCollector, CobolRecordConverterConfig.PROPERTY_CHARSET);
  }

  @Test
  public void testInvalidCopybook() {
    CobolRecordConverterConfig config = CobolRecordConverterConfig.builder(VALID_CONFIG)
      .setCopybook("invalidCopybook")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    try {
      config.getOutputSchemaAndValidate(failureCollector, VALID_SCHEMA);
      Assert.fail("ValidationException exception was expected, but not thrown.");
    } catch (ValidationException ex) {
    }

    assertValidationFailed(failureCollector, CobolRecordConverterConfig.PROPERTY_COPYBOOK);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();

    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.STAGE_CONFIG);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(paramName, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Nonnull
  private static List<ValidationFailure.Cause> getCauses(ValidationFailure failure, String stacktrace) {
    return failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(stacktrace) != null)
      .collect(Collectors.toList());
  }
}
