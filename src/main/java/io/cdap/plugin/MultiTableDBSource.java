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

package io.cdap.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSource;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.format.DBTableInfo;
import io.cdap.plugin.format.MultiSQLStatementInputFormat;
import io.cdap.plugin.format.MultiTableConf;
import io.cdap.plugin.format.MultiTableDBInputFormat;
import io.cdap.plugin.format.RecordWrapper;
import io.cdap.plugin.format.error.TableFailureException;
import io.cdap.plugin.format.error.collector.ErrorCollectingMultiSQLStatementInputFormat;
import io.cdap.plugin.format.error.collector.ErrorCollectingMultiTableDBInputFormat;
import io.cdap.plugin.format.error.emitter.ErrorEmittingInputFormat;
import io.cdap.plugin.util.FQNGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Batch source to read from multiple tables in a database using JDBC.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("MultiTableDatabase")
@Description("Reads from multiple tables in a relational database. " +
  "Outputs one record for each row in each table, with the table name as a record field. " +
  "Also sets a pipeline argument for each table read, which contains the table schema. ")
public class MultiTableDBSource extends ReferenceBatchSource<NullWritable, RecordWrapper, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTableDBSource.class);

  private static final String JDBC_PLUGIN_ID = "jdbc.driver";

  private final MultiTableConf conf;

  public MultiTableDBSource(MultiTableConf conf) {
    super(new ReferencePluginConfig(conf.getReferenceName()));
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Class<? extends Driver> jdbcDriverClass = pipelineConfigurer.usePluginClass("jdbc", conf.getJdbcPluginName(),
                                                                                JDBC_PLUGIN_ID,
                                                                                PluginProperties.builder().build());
    if (jdbcDriverClass == null) {
      throw new IllegalArgumentException(
        String.format("Unable to load JDBC Driver class for plugin name '%s'. " +
                        "Please make sure that the driver plugin has been installed correctly.",
                      conf.getJdbcPluginName()));
    }

    pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Configuration hConf = new Configuration();
    Class<? extends Driver> driverClass = context.loadPluginClass(JDBC_PLUGIN_ID);

    try {
      if (MultiTableConf.DATA_SELECTION_MODE_ALLOW_LIST.equals(conf.getDataSelectionMode())
        || MultiTableConf.DATA_SELECTION_MODE_BLOCK_LIST.equals(conf.getDataSelectionMode())) {
        //Proceed with Multi DB Input
        setContextForMultiTableDBInput(context, hConf, driverClass);
      } else {
        //Proceed with Multi SQL Statement Input
        setContextForMultiSQLStatementInput(context, hConf, driverClass);
      }
    } catch (Exception ex) {
      String errorMessage = "Error getting table schemas from database.";
      LOG.error(errorMessage, ex);

      InputFormatProvider errorEmittingInputFormatProvider = new InputFormatProvider() {
        @Override
        public String getInputFormatClassName() {
          return ErrorEmittingInputFormat.class.getName();
        }

        @Override
        public Map<String, String> getInputFormatConfiguration() {
          Map<String, String> config = new HashMap<>();
          for (Map.Entry<String, String> entry : hConf) {
            config.put(entry.getKey(), entry.getValue());
          }
          //Add ErrorEmittingInputFormat config properties
          config.put(ErrorEmittingInputFormat.ERROR_MESSAGE, errorMessage);
          config.put(ErrorEmittingInputFormat.EXCEPTION_CLASS_NAME, ex.getClass().getCanonicalName());

          return config;
        }
      };

      context.setInput(Input.of(conf.getReferenceName(), errorEmittingInputFormatProvider));
    }
  }

  @Override
  public void transform(KeyValue<NullWritable, RecordWrapper> input, Emitter<StructuredRecord> emitter) {
    RecordWrapper wrapper = input.getValue();

    // Check if the record is an error.
    if (wrapper.isError()) {
      // Fail the pipeline if the table error threshold exceeds the desired number of table failures.
      if (MultiTableConf.ERROR_HANDLING_FAIL_PIPELINE.equals(conf.getErrorHandlingMode())) {
        throw new TableFailureException();
      }

      // Emit error record through error port if configured to do so.
      if (MultiTableConf.ERROR_HANDLING_SEND_TO_ERROR_PORT.equals(conf.getErrorHandlingMode())) {
        emitter.emitError(wrapper.getInvalidEntry());
      }

      return;
    }

    emitter.emit(wrapper.getRecord());
  }

  public void setContextForMultiTableDBInput(BatchSourceContext context,
                                             Configuration hConf, Class<? extends Driver> driverClass)
    throws IllegalAccessException, SQLException, InstantiationException {

    Collection<DBTableInfo> tables;

    tables = MultiTableDBInputFormat.setInput(hConf, conf, driverClass);

    SettableArguments arguments = context.getArguments();
    for (DBTableInfo tableInfo : tables) {
      Schema schema = tableInfo.getSchema();
      arguments.set(DynamicMultiFilesetSink.TABLE_PREFIX + tableInfo.getDbTableName().getTable(),
                    schema.toString());
      emitLineage(context, tableInfo, schema);
    }

    context.setInput(Input.of(conf.getReferenceName(),
                              new SourceInputFormatProvider(ErrorCollectingMultiTableDBInputFormat.class, hConf)));
  }

  private void emitLineage(BatchSourceContext context, DBTableInfo tableInfo, Schema schema) {
    Asset asset = Asset.builder(conf.getReferenceName())
      .setFqn(FQNGenerator.constructFQN(conf.getConnectionString(), tableInfo.getDbTableName().getTable()))
      .setMarker(tableInfo.getDbTableName().getTable()).build();
    LineageRecorder lineageRecorder = new LineageRecorder(context, asset);
    lineageRecorder.createExternalDataset(schema);
    if (schema != null && schema.getFields() != null) {
      String operationName = "Read_from_" + tableInfo.getDbTableName().getTable();
      lineageRecorder.recordRead(operationName, "Read from database plugin",
                                 schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  public void setContextForMultiSQLStatementInput(BatchSourceContext context,
                                                  Configuration hConf, Class<? extends Driver> driverClass) {
    MultiSQLStatementInputFormat.setInput(hConf, conf, driverClass);

    context.setInput(Input.of(conf.getReferenceName(),
                              new SourceInputFormatProvider(ErrorCollectingMultiSQLStatementInputFormat.class, hConf)));
  }
}
