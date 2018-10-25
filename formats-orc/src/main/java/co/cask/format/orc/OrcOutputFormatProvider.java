/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.format.orc;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.HiveSchemaConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Output format plugin for ORC.
 */
@Plugin(type = "outputformat")
@Name("orc")
@Description("ORC output format plugin that provides the output format class name and properties "
  + "required to write ORC files.")
public class OrcOutputFormatProvider implements OutputFormatProvider {
  private static final String ORC_COMPRESS = "orc.compress";
  private static final String SNAPPY_CODEC = "SNAPPY";
  private static final String ZLIB_CODEC = "ZLIB";
  private static final String COMPRESS_SIZE = "orc.compress.size";
  private static final String ROW_INDEX_STRIDE = "orc.row.index.stride";
  private static final String CREATE_INDEX = "orc.create.index";
  private final Conf conf;

  public OrcOutputFormatProvider(Conf conf) {
    this.conf = conf;
  }

  @Override
  public String getOutputFormatClassName() {
    return StructuredOrcOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    Map<String, String> configuration = new HashMap<>();
    configuration.put("orc.mapred.output.schema", parseOrcSchema(conf.schema));

    if (conf.compressionCodec != null && !conf.compressionCodec.equalsIgnoreCase("None")) {
      switch (conf.compressionCodec.toUpperCase()) {
        case SNAPPY_CODEC:
          configuration.put(ORC_COMPRESS, SNAPPY_CODEC);
          break;
        case ZLIB_CODEC:
          configuration.put(ORC_COMPRESS, ZLIB_CODEC);
          break;
        default:
          throw new IllegalArgumentException("Unsupported compression codec " + conf.compressionCodec);
      }
      if (conf.compressionChunkSize != null) {
        configuration.put(COMPRESS_SIZE, conf.compressionChunkSize.toString());
      }
      if (conf.stripeSize != null) {
        configuration.put(COMPRESS_SIZE, conf.stripeSize.toString());
      }
      if (conf.indexStride != null) {
        configuration.put(ROW_INDEX_STRIDE, conf.indexStride.toString());
      }
      if (conf.createIndex != null) {
        configuration.put(CREATE_INDEX, conf.indexStride.toString());
      }
    }
    return configuration;
  }

  /**
   * Configuration for the output format plugin.
   */
  public static class Conf extends PluginConfig {
    @Macro
    @Description("Schema of the data to write.")
    private String schema;

    @Macro
    @Nullable
    @Description("Used to specify the compression codec to be used for the final dataset.")
    private String compressionCodec;

    @Macro
    @Nullable
    @Description("Number of bytes in each compression chunk.")
    private Long compressionChunkSize;

    @Macro
    @Nullable
    @Description("Number of bytes in each stripe.")
    private Long stripeSize;

    @Macro
    @Nullable
    @Description("Number of rows between index entries (must be >= 1,000)")
    private Long indexStride;

    @Macro
    @Nullable
    @Description("Whether to create inline indexes")
    private Boolean createIndex;
  }

  private static String parseOrcSchema(String configuredSchema) {
    try {
      co.cask.cdap.api.data.schema.Schema schemaObj = co.cask.cdap.api.data.schema.Schema.parseJson(configuredSchema);
      StringBuilder builder = new StringBuilder();
      HiveSchemaConverter.appendType(builder, schemaObj);
      return builder.toString();
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("%s is not a valid schema", configuredSchema), e);
    } catch (UnsupportedTypeException e) {
      throw new IllegalArgumentException(String.format("Could not create hive schema from %s", configuredSchema), e);
    }
  }
}
