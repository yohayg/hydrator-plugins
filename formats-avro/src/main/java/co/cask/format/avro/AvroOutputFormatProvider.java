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

package co.cask.format.avro;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.plugin.PluginConfig;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Output format plugin for avro.
 */
@Plugin(type = "outputformat")
@Name("avro")
@Description("Avro output format plugin that provides the output format class name and properties "
  + "required to write Avro files.")
public class AvroOutputFormatProvider implements OutputFormatProvider {
  static final String SCHEMA_KEY = "avro.schema.output.key";
  private static final String AVRO_OUTPUT_CODEC = "avro.output.codec";
  private static final String MAPRED_OUTPUT_COMPRESS = "mapred.output.compress";
  private static final String CODEC_SNAPPY = "snappy";
  private static final String CODEC_DEFLATE = "deflate";
  private final Conf conf;

  public AvroOutputFormatProvider(Conf conf) {
    this.conf = conf;
  }

  @Override
  public String getOutputFormatClassName() {
    return StructuredAvroOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    Map<String, String> configuration = new HashMap<>();
    if (conf.schema != null) {
      configuration.put(SCHEMA_KEY, conf.schema);
    }

    if (conf.compressionCodec != null && !conf.containsMacro("compressionCodec") &&
      !"none".equalsIgnoreCase(conf.compressionCodec)) {

      configuration.put(MAPRED_OUTPUT_COMPRESS, "true");
      if (CODEC_SNAPPY.equalsIgnoreCase(conf.compressionCodec)) {
        configuration.put(AVRO_OUTPUT_CODEC, CODEC_SNAPPY);
      } else if (CODEC_DEFLATE.equalsIgnoreCase(conf.compressionCodec)) {
        configuration.put(AVRO_OUTPUT_CODEC, CODEC_DEFLATE);
      } else {
        throw new IllegalArgumentException("Unsupported compression codec " + conf.compressionCodec);
      }
    }
    return configuration;
  }

  /**
   * Configuration for the output format plugin.
   */
  public static class Conf extends PluginConfig {

    @Macro
    @Nullable
    @Description("schema of the data to write.")
    private String schema;

    @Macro
    @Nullable
    @Description("compression codec to use when writing data. Must be 'snappy', 'deflate', or 'none'.")
    private String compressionCodec;
  }
}
