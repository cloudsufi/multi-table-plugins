/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.format.error.collector;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.plugin.format.DBTableSplit;
import io.cdap.plugin.format.MultiTableDBConfiguration;
import io.cdap.plugin.format.MultiTableDBInputFormat;
import io.cdap.plugin.format.RecordWrapper;
import io.cdap.plugin.format.error.emitter.ErrorEmittingInputSplit;
import io.cdap.plugin.format.error.emitter.ErrorEmittingRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Input Format that handles exceptions in the delegate MultiTableDBInputFormat and routes them through an emitter.
 * This allows us to send errors via the Error Emitter for further processing.
 */
public class ErrorCollectingMultiTableDBInputFormat extends InputFormat<NullWritable, RecordWrapper> {
  private static final Logger LOG = LoggerFactory.getLogger(ErrorCollectingMultiTableDBInputFormat.class);

  private final InputFormat<NullWritable, RecordWrapper> delegate;

  public ErrorCollectingMultiTableDBInputFormat() {
    this.delegate = new MultiTableDBInputFormat();
  }

  @VisibleForTesting
  public ErrorCollectingMultiTableDBInputFormat(InputFormat<NullWritable, RecordWrapper> delegate) {
    this.delegate = delegate;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    try {
      LOG.info("Creating splits for MultiTableDBInputFormat");
      // log delegate class name
      LOG.info("Delegate class name: {}", delegate.getClass().getName());
      return delegate.getSplits(context);
    } catch (IOException | InterruptedException e) {
      // If there was an exception creating the splits, we create a single ErrorEmittingInputSplit in order to
      LOG.error("Exception creating splits", e);

      MultiTableDBConfiguration conf = new MultiTableDBConfiguration(context.getConfiguration());
      InputSplit errorSplit = new ErrorEmittingInputSplit(conf.getPluginConf().getReferenceName(),
                                                          "Exception creating splits",
                                                          e.getClass().getCanonicalName());
      return Collections.singletonList(errorSplit);
    }
  }

  @Override
  public RecordReader<NullWritable, RecordWrapper> createRecordReader(InputSplit split, TaskAttemptContext context) {
    // Handle the scenario where the Input Split is already an Error Emitting Input Split.
    // In this case, we supply the error message and exception class name to the ErrorEmittingRecordReader.
    if (split instanceof ErrorEmittingInputSplit) {
      ErrorEmittingInputSplit errorSplit = (ErrorEmittingInputSplit) split;
      return new ErrorEmittingRecordReader(errorSplit.getReferenceName(),
                                           errorSplit.getErrorMessage(),
                                           errorSplit.getExceptionClassName());
    }

    try {
      //Get configuration
      MultiTableDBConfiguration multiTableDBConf = new MultiTableDBConfiguration(context.getConfiguration());

      //Get table name from Input Split
      DBTableSplit dbTableSplit = (DBTableSplit) split;
      String tableName = dbTableSplit.getTableName().fullTableName();

      //Delegate record reader creation
      RecordReader<NullWritable, RecordWrapper> reader = delegate.createRecordReader(split, context);

      //Wrap record reader in the error collecting record reader.
      return new ErrorCollectingRecordReader(multiTableDBConf.getPluginConf().getReferenceName(), reader, tableName);
    } catch (Exception e) {
      return getErrorEmittingRecordReader(split, context, e);
    }
  }

  protected RecordReader<NullWritable, RecordWrapper> getErrorEmittingRecordReader(InputSplit split,
                                                                                   TaskAttemptContext context,
                                                                                   Exception e) {
    MultiTableDBConfiguration multiTableDBConf = new MultiTableDBConfiguration(context.getConfiguration());
    DBTableSplit dbTableSplit = (DBTableSplit) split;

    String errorMessage = String.format("Error creating splits for table '%s'.",
                                        dbTableSplit.getTableName().fullTableName());
    LOG.error(errorMessage, e);

    return new ErrorEmittingRecordReader(multiTableDBConf.getPluginConf().getReferenceName(),
                                         errorMessage,
                                         e.getClass().getCanonicalName(),
                                         dbTableSplit.getTableName().fullTableName());
  }
}
