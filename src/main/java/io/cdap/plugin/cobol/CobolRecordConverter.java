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

import com.legstar.avro.cob2avro.io.AbstractZosDatumReader;
import com.legstar.cob2xsd.Cob2XsdConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.plugin.EndpointPluginContext;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.common.AvroConverter;
import io.cdap.plugin.common.StreamByteSource;
import io.cdap.plugin.common.StreamCharSource;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * {@link Transform} plugin to convert COBOL data file into StructuredRecords.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("CobolRecordConverter")
@Description("Convert COBOL records into StructuredRecord with schema.")
public class CobolRecordConverter extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(CobolRecordConverter.class);

  private final Config config;

  public CobolRecordConverter(Config config) {
    this.config = config;
  }

  private CopybookReader copybookReader;
  private Schema avroSchema;
  private io.cdap.cdap.api.data.schema.Schema schema;

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    Properties properties = new Properties();
    properties.setProperty(Cob2XsdConfig.CODE_FORMAT, config.getCodeFormat());
    StreamCharSource streamCharSource
      = new StreamCharSource(new ByteArrayInputStream(config.copybook.getBytes(StandardCharsets.UTF_8)));
    copybookReader = new CopybookReader(streamCharSource, properties);
    this.avroSchema = copybookReader.getSchema();
    this.schema = AvroConverter.fromAvroSchema(avroSchema);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    byte[] body = input.get(config.fieldName);
    StreamByteSource source = new StreamByteSource(new ByteArrayInputStream(body), body.length);
    try (AbstractZosDatumReader<GenericRecord> reader = copybookReader.createRecordReader(source, config.getCharset(),
                                                                                          config.hasRDW())) {
      for (GenericRecord record : reader) {
        LOG.info(StructuredRecordStringConverter.toJsonString(AvroConverter.fromAvroRecord(record, schema)));
        emitter.emit(AvroConverter.fromAvroRecord(record, schema));
      }
    }
  }

  class GetSchemaRequest {
    public String copybook;
    @Nullable
    public String codeFormat;

    private String getCodeFormat() {
      return codeFormat == null ? Cob2XsdConfig.CodeFormat.FIXED_FORMAT.name() : codeFormat;
    }
  }

  /**
   * Endpoint method to get the output schema given copybook.
   *
   * @param request {@link GetSchemaRequest} containing information about the cobol copybook.
   * @param pluginContext context to create plugins
   * @return schema of fields
   * @throws IOException if there are any errors converting schema
   */
  @javax.ws.rs.Path("outputSchema")
  public io.cdap.cdap.api.data.schema.Schema getSchema(GetSchemaRequest request,
                                                       EndpointPluginContext pluginContext) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(Cob2XsdConfig.CODE_FORMAT, request.getCodeFormat());
    StreamCharSource streamCharSource
      = new StreamCharSource(new ByteArrayInputStream(request.copybook.getBytes(StandardCharsets.UTF_8)));
    CopybookReader reader = new CopybookReader(streamCharSource, properties);
    Schema avroSchema  = reader.getSchema();
    return AvroConverter.fromAvroSchema(avroSchema);
  }

  public static final class Config extends PluginConfig {
    @Description("COBOL Copybook")
    @Macro
    private String copybook;

    @Description("CodeFormat in the Copybook")
    @Nullable
    private String codeFormat;

    @Description("Charset used to read the data. Default Charset is 'IBM01140'.")
    @Nullable
    private String charset;

    @Description("Records start with Record Descriptor Word")
    @Nullable
    private Boolean rdw;

    @Description("Name of the field containing COBOL records")
    private String fieldName;

    public String getCodeFormat() {
      return codeFormat == null ? Cob2XsdConfig.CodeFormat.FIXED_FORMAT.name() : codeFormat;
    }

    public String getCharset() {
      return charset == null ? "IBM01140" : charset;
    }

    public boolean hasRDW() {
      return rdw == null ? true : rdw;
    }
  }
}
