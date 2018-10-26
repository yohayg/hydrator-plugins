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

package co.cask.format.delimited;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;

import java.util.Map;

/**
 * Output format plugin for csv.
 */
@Plugin(type = "outputformat")
@Name("csv")
@Description("CSV output format plugin that provides the output format class name and properties "
  + "required to write csv files.")
public class CSVOutputFormatProvider implements OutputFormatProvider {

  @Override
  public String getOutputFormatClassName() {
    return StructuredDelimitedOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return StructuredDelimitedOutputFormat.getConfiguration(",");
  }
}
