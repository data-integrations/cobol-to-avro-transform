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

import com.legstar.cob2xsd.Cob2XsdConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.AvroConverter;
import io.cdap.plugin.common.StreamCharSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import javax.annotation.Nullable;

public class CobolRecordConverterConfig extends PluginConfig {
  public static final String PROPERTY_COPYBOOK = "copybook";
  public static final String PROPERTY_CONTENT_FIELD_NAME = "contentFieldName";
  public static final String PROPERTY_CHARSET = "charset";

  @Name(PROPERTY_COPYBOOK)
  @Description("COBOL Copybook")
  @Macro
  private final String copybook;

  @Description("CodeFormat in the Copybook")
  @Nullable
  private final String codeFormat;

  @Name(PROPERTY_CHARSET)
  @Description("Charset used to read the data. Default Charset is 'IBM01140'.")
  @Nullable
  private final String charset;

  @Description("Records start with Record Descriptor Word")
  @Nullable
  private final Boolean rdw;

  @Name(PROPERTY_CONTENT_FIELD_NAME)
  @Description("Name of the field containing COBOL records")
  private final String contentFieldName;

  public CobolRecordConverterConfig(String copybook, @Nullable String codeFormat, @Nullable String charset,
                                    @Nullable Boolean rdw, String contentFieldName) {
    this.copybook = copybook;
    this.codeFormat = codeFormat;
    this.charset = charset;
    this.rdw = rdw;
    this.contentFieldName = contentFieldName;
  }

  private CobolRecordConverterConfig(Builder builder) {
    this.copybook = builder.copybook;
    this.codeFormat = builder.codeFormat;
    this.charset = builder.charset;
    this.rdw = builder.rdw;
    this.contentFieldName = builder.contentFieldName;
  }

  public String getCopybook() {
    return copybook;
  }

  public String getCodeFormat() {
    return codeFormat == null ? Cob2XsdConfig.CodeFormat.FIXED_FORMAT.name() : codeFormat;
  }

  public String getCharset() {
    return charset == null ? "IBM01140" : charset;
  }

  public boolean hasRDW() {
    return rdw == null ? true : rdw;
  }

  public String getContentFieldName() {
    return contentFieldName;
  }

  public byte[] getCopybookBytes() {
    return copybook.getBytes(StandardCharsets.UTF_8);
  }

  public Schema getOutputSchemaAndValidate(FailureCollector failureCollector, Schema inputSchema) {
    Schema.Field contentField = inputSchema.getField(contentFieldName);
    if (contentField == null) {
      failureCollector.addFailure(String.format("Field '%s' is not present in input schema.", contentFieldName),
                                  null).withConfigProperty(PROPERTY_CONTENT_FIELD_NAME)
        .withInputSchemaField(PROPERTY_CONTENT_FIELD_NAME, null);
    } else {
      Schema contentFieldSchema = contentField.getSchema();

      if (contentFieldSchema.isNullable()) {
        contentFieldSchema = contentFieldSchema.getNonNullable();
      }

      if (contentFieldSchema.getLogicalType() != null || contentFieldSchema.getType() != Schema.Type.BYTES) {
        failureCollector.addFailure(String.format("Field '%s' must be of type 'bytes' but is of type '%s'.",
                                                  contentField.getName(), contentFieldSchema.getDisplayName()),
                                    null).withConfigProperty(PROPERTY_CONTENT_FIELD_NAME)
          .withInputSchemaField(PROPERTY_CONTENT_FIELD_NAME, null);
      }
    }

    if (!Charset.isSupported(getCharset())) {
      failureCollector.addFailure(String.format("The charset name '%s' is not supported by your java environment.",
                                                getCharset()),
                                  "Make sure you have lib/charsets.jar in your jre.")
        .withConfigProperty(PROPERTY_CHARSET);
      // if above failed, we cannot proceed to copybook parsing.
      throw failureCollector.getOrThrowException();
    }

    CopybookReader copybookReader;
    try {
      copybookReader = getCopybookReader();
    } catch(Exception ex) {
      failureCollector.addFailure(String.format("Error while reading copybook: '%s'", ex.getMessage()),
                                  "Please make sure it has correct format")
        .withConfigProperty(PROPERTY_COPYBOOK)
        .withStacktrace(ex.getStackTrace());
      throw failureCollector.getOrThrowException();
    }

    try {
      return getOutputSchemaAndValidate(copybookReader);
    } catch(Exception ex) {
      failureCollector.addFailure(String.format("Error while generating schema from the copybook: '%s'",
                                                ex.getMessage()), null)
        .withConfigProperty(PROPERTY_COPYBOOK)
        .withStacktrace(ex.getStackTrace());
      throw failureCollector.getOrThrowException();
    }
  }

  public CopybookReader getCopybookReader() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(Cob2XsdConfig.CODE_FORMAT, getCodeFormat());
    StreamCharSource streamCharSource
      = new StreamCharSource(new ByteArrayInputStream(getCopybookBytes()));
    return new CopybookReader(streamCharSource, properties);
  }

  public Schema getOutputSchemaAndValidate(CopybookReader copybookReader) {
    org.apache.avro.Schema avroSchema = copybookReader.getSchema();
    return AvroConverter.fromAvroSchema(avroSchema);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(CobolRecordConverterConfig copy) {
    return new Builder()
      .setCopybook(copy.getCopybook())
      .setCodeFormat(copy.getCodeFormat())
      .setCharset(copy.getCharset())
      .setRdw(copy.hasRDW())
      .setContentFieldName(copy.getContentFieldName());
  }

  public static final class Builder {
    private String copybook;
    private String codeFormat;
    private String charset;
    private Boolean rdw;
    private String contentFieldName;

    public Builder setCopybook(String copybook) {
      this.copybook = copybook;
      return this;
    }

    public Builder setCodeFormat(String codeFormat) {
      this.codeFormat = codeFormat;
      return this;
    }

    public Builder setCharset(String charset) {
      this.charset = charset;
      return this;
    }

    public Builder setRdw(Boolean rdw) {
      this.rdw = rdw;
      return this;
    }

    public Builder setContentFieldName(String contentFieldName) {
      this.contentFieldName = contentFieldName;
      return this;
    }

    private Builder() {
    }

    public CobolRecordConverterConfig build() {
      return new CobolRecordConverterConfig(this);
    }
  }
}
