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

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.google.common.base.Joiner;
import com.google.common.io.ByteSource;
import com.google.common.io.CharSource;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;
import com.legstar.avro.cob2avro.io.AbstractZosDatumReader;
import com.legstar.avro.cob2avro.io.ZosVarDatumReader;
import com.legstar.avro.cob2avro.io.ZosVarRdwDatumReader;
import com.legstar.avro.translator.Xsd2AvroTranslator;
import com.legstar.avro.translator.Xsd2AvroTranslatorException;
import com.legstar.base.context.EbcdicCobolContext;
import com.legstar.base.generator.Xsd2CobolTypesModelBuilder;
import com.legstar.base.type.CobolType;
import com.legstar.base.type.composite.CobolComplexType;
import com.legstar.cob2xsd.Cob2Xsd;
import com.legstar.cob2xsd.Cob2XsdConfig;
import com.legstar.cob2xsd.antlr.RecognizerException;
import com.legstar.cobol.model.CobolDataItem;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaSerializer;
import org.codehaus.janino.JavaSourceClassLoader;
import org.codehaus.janino.util.resource.Resource;
import org.codehaus.janino.util.resource.ResourceFinder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * This class helps parsing Cobol Copybook and decoding Ebcdic encoded dataset
 */
public class CopybookReader {

  private final Schema avroSchema;
  private final ClassLoader cobolTypeClassLoader;

  public CopybookReader(CharSource copybookSource, Properties cobolConfig) throws IOException {
    Cob2Xsd cob2xsd = new Cob2Xsd(new Cob2XsdConfig(cobolConfig));

    try (Reader reader = copybookSource.openStream()) {
      // Parse the copybook
      List<CobolDataItem> cobolDataItems = parseCopybook(cob2xsd, reader);

      // Generate XML schema from the copybook
      XmlSchema xmlSchema = new XmlSchemaCollection().read(
        cob2xsd.emitXsd(cobolDataItems, "io.cdap.plugin.cobol").getSchemaDocument());

      // Convert XML schema to Avro schema
      Schema avroSchema = translate(xmlSchema);

      // Generate the CobolType classes ClassLoader
      this.cobolTypeClassLoader = createCobolTypesClassLoader(xmlSchema, "io.cdap.plugin.cobol");
      this.avroSchema = avroSchema;
    } catch (RecognizerException e) {
      throw new IOException("Failed to parse cobol copybook: " + System.lineSeparator()
                              + Joiner.on(System.lineSeparator()).join(cob2xsd.getErrorHistory()), e);
    } catch (XmlSchemaSerializer.XmlSchemaSerializerException | Xsd2AvroTranslatorException e) {
      throw new IOException("Failed to generate Avro schema from cobol copybook", e);
    }
  }

  /**
   * Returns all Avro schema created from the Cobol copybook
   *
   * @return a {@link Map} from record name to record {@link Schema}.
   */
  public Schema getSchema() {
    return avroSchema;
  }

  /**
   * Creates a {@link AbstractZosDatumReader} for reading Ebcdic encoded dataset into Avro {@link GenericRecord}.
   *
   * @param source The {@link ByteSource} for the dataset
   * @param charset The charset used to create EBCDIC COBOL context
   * @param hasRecordDescriptorWord {@code true} for data that has the record descriptor word prefix for each record;
   *                                {@code false} other
   * @return A {@link AbstractZosDatumReader} for reading
   * @throws IOException If failed to create the reader
   */
  public AbstractZosDatumReader<GenericRecord> createRecordReader(ByteSource source, String charset,
                                                                  boolean hasRecordDescriptorWord) throws IOException {
    String cobolTypeClassName = avroSchema.getNamespace() + "." + avroSchema.getName();
    CobolComplexType cobolType;
    try {
      cobolType = (CobolComplexType) cobolTypeClassLoader.loadClass(cobolTypeClassName).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      // This shouldn't happen since we generated the class
      throw new IOException("Failed to instantiate instance of class " + cobolTypeClassName, e);
    }

    long size = source.size();
    InputStream is = source.openBufferedStream();
    try {
      if (hasRecordDescriptorWord) {
        return new ZosVarRdwDatumReader<>(is, size, new EbcdicCobolContext(charset), cobolType, avroSchema);
      }
      return new ZosVarDatumReader<>(is, size, new EbcdicCobolContext(charset), cobolType, avroSchema);
    } catch (IOException e) {
      Closeables.closeQuietly(is);
      throw e;
    }
  }

  private List<CobolDataItem> parseCopybook(Cob2Xsd cob2xsd, Reader reader) throws RecognizerException {
    Cob2XsdConfig config = cob2xsd.getConfig();

    List<CobolDataItem> cobolDataItems = new ArrayList<>();
    for (CobolDataItem item : cob2xsd.toModel(reader)) {
      if (config.ignoreOrphanPrimitiveElements() && item.getChildren().isEmpty()) {
        continue;
      }
      cobolDataItems.add(item);
    }

    // If the copybook is empty, the list would be empty.
    // If the copybook has top level record, the size of the list would be 1.
    if (cobolDataItems.size() <= 1) {
      return cobolDataItems;
    }

    // If the copybook doesn't have top level record, insert one
    CobolDataItem item = new CobolDataItem(1, "GENERATED-TOP-RECORD");
    item.setChildren(cobolDataItems);

    cobolDataItems = new ArrayList<>();
    cobolDataItems.add(item);

    return cobolDataItems;
  }

  /**
   * Translates a {@link XmlSchema} into a Avro {@link Schema}.
   *
   * @param xmlSchema the {@link XmlSchema} to translate from
   * @return a avro record {@link Schema}
   * @throws Xsd2AvroTranslatorException if translation failed
   */
  private Schema translate(XmlSchema xmlSchema) throws Xsd2AvroTranslatorException {
    Xsd2AvroTranslator avroTranslator = new Xsd2AvroTranslator();
    return new Schema.Parser().parse(avroTranslator.translate(xmlSchema, "io.cdap.plugin.cobol", "schema"));
  }

  /**
   * Creates a {@link ClassLoader} for loading {@link CobolType} classes that can be used for reading data encoded using
   * Cobol copybook.
   *
   * @param xmlSchema The {@link XmlSchema} representation of the Cobol copybook.
   * @param classPackage Name of the java package for the generated classes to locates in.
   * @return a {@link ClassLoader} for loading {@link CobolType} classes
   * @throws IOException if failed to create the ClassLoader
   */
  private ClassLoader createCobolTypesClassLoader(XmlSchema xmlSchema, String classPackage) throws IOException {
    final Map<String, String> sources = generateCobolTypes(xmlSchema, classPackage);
    final long lastModified = System.currentTimeMillis();
    return new JavaSourceClassLoader(getClass().getClassLoader(), new ResourceFinder() {

      @Nullable
      @Override
      public Resource findResource(final String resourceName) {
        String className = resourceName.replace('/', '.').substring(0, resourceName.length() - ".java".length());
        final String sourceCode = sources.get(className);
        if (sourceCode == null) {
          return null;
        }
        return new Resource() {
          @Override
          public InputStream open() throws IOException {
            return new ByteArrayInputStream(sourceCode.getBytes("UTF-8"));
          }

          @Override
          public String getFileName() {
            return resourceName;
          }

          @Override
          public long lastModified() {
            return lastModified;
          }
        };
      }
    }, null);
  }

  /**
   * Generates the source code of different {@link CobolType} that can be used for reading data encoded using
   * Cobol copybook.
   *
   * @param xmlSchema The {@link XmlSchema} representation of the Cobol copybook.
   * @param classPackage Name of the java package for the generated classes to locates in.
   * @return A {@link Map} from class name to class source code
   * @throws IOException if failed to generate the classes
   */
  private Map<String, String> generateCobolTypes(XmlSchema xmlSchema, String classPackage) throws IOException {
    URL resource = getClass().getClassLoader().getResource("java.class.hbs");
    if (resource == null) {
      // This shouldn't happen
      throw new IllegalStateException("Resource not found: java.class.hbs");
    }

    Handlebars handlebars = new Handlebars();
    Template template = handlebars.compileInline(Resources.toString(resource, StandardCharsets.UTF_8));
    Map<String, Xsd2CobolTypesModelBuilder.RootCompositeType> model = new Xsd2CobolTypesModelBuilder().build(xmlSchema);

    Map<String, String> sources = new HashMap<>();
    for (Map.Entry<String, Xsd2CobolTypesModelBuilder.RootCompositeType> entry : model.entrySet()) {
      String className = entry.getKey();
      Map<String, Object> config = new HashMap<>();
      config.put("target_package_name", classPackage);
      config.put("class_name", className);
      config.put("root_type_name", entry.getKey());
      config.put("root_cobol_name", entry.getValue().cobolName);
      config.put("complex_types", entry.getValue().complexTypes);
      config.put("choice_types", entry.getValue().choiceTypes);
      sources.put(classPackage + "." + className, template.apply(config));
    }
    return sources;
  }
}
