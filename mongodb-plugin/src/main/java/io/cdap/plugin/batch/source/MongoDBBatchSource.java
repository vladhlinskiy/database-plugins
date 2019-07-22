/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.source;

import com.google.common.base.Strings;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.splitter.StandaloneMongoSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.BSONConverter;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSource;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.common.SourceInputFormatProvider;
import org.apache.hadoop.conf.Configuration;
import org.bson.BSONObject;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} that reads data from MongoDB and converts each document into 
 * a {@link StructuredRecord} with the help of the specified Schema.
 */
@Plugin(type = "batchsource")
@Name("MongoDB")
@Description("MongoDB Batch Source will read documents from MongoDB and convert each document " +
  "into a StructuredRecord with the help of the specified Schema. ")
public class MongoDBBatchSource extends ReferenceBatchSource<Object, BSONObject, StructuredRecord> {

  private final MongoDBConfig config;
  private BSONConverter bsonConverter;

  public MongoDBBatchSource(MongoDBConfig config) {
    super(new ReferencePluginConfig(config.referenceName));
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Schema schema = config.getSchema();
    BSONConverter.validateSchema(schema);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Configuration conf = new Configuration();
    conf.clear();

    MongoConfigUtil.setInputFormat(conf, MongoInputFormat.class);
    MongoConfigUtil.setInputURI(conf, config.getConnectionString());
    if (!Strings.isNullOrEmpty(config.inputQuery)) {
      MongoConfigUtil.setQuery(conf, config.inputQuery);
    }
    if (!Strings.isNullOrEmpty(config.authConnectionString)) {
      MongoConfigUtil.setAuthURI(conf, config.authConnectionString);
    }
    if (!Strings.isNullOrEmpty(config.inputFields)) {
      MongoConfigUtil.setFields(conf, config.inputFields);
    }
    if (!Strings.isNullOrEmpty(config.splitterClass)) {
      String className = String.format("%s.%s", StandaloneMongoSplitter.class.getPackage().getName(),
                                       config.splitterClass);
      Class<? extends MongoSplitter> klass = getClass().getClassLoader().loadClass(
        className).asSubclass(MongoSplitter.class);
      MongoConfigUtil.setSplitterClass(conf, klass);
    }
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());
    context.setInput(Input.of(config.referenceName,
                              new SourceInputFormatProvider(MongoConfigUtil.getInputFormat(conf), conf)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    bsonConverter = new BSONConverter(config.getSchema());
  }

  @Override
  public void transform(KeyValue<Object, BSONObject> input, Emitter<StructuredRecord> emitter) throws Exception {
    BSONObject bsonObject = input.getValue();
    emitter.emit(bsonConverter.transform(bsonObject));
  }

  /**
   * Config class for {@link MongoDBBatchSource}.
   */
  public static class MongoDBConfig extends PluginConfig {

    @Name(Constants.Reference.REFERENCE_NAME)
    @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
    public String referenceName;

    @Name(Properties.HOST)
    @Description("Host that MongoDB is running on.")
    public String host;

    @Name(Properties.PORT)
    @Description("Port that MongoDB is listening to.")
    public Integer port;

    @Name(Properties.DATABASE)
    @Description("MongoDB database name.")
    public String database;

    @Name(Properties.COLLECTION)
    @Description("Name of the database collection to read from.")
    public String collection;

    @Name(Properties.SCHEMA)
    @Description("Schema of records output by the source.")
    private String schema;

    @Name(Properties.INPUT_QUERY)
    @Description("Optionally filter the input collection with a query. This query must be represented in JSON " +
      "format, and use the MongoDB extended JSON format to represent non-native JSON data types.")
    @Nullable
    @Macro
    private String inputQuery;

    @Name(Properties.INPUT_FIELDS)
    @Nullable
    @Description("A projection document limiting the fields that appear in each document. " +
      "If no projection document is provided, all fields will be read.")
    @Macro
    private String inputFields;

    @Name(Properties.SPLITTER_CLASS)
    @Nullable
    @Description("The name of the Splitter class to use. If left empty, the MongoDB Hadoop Connector will attempt " +
      "to make a best guess as to what Splitter to use.")
    @Macro
    private String splitterClass;

    @Name(Properties.USER)
    @Description("User to use to connect to the specified database. Required for databases that " +
      "need authentication. Optional for databases that do not require authentication.")
    @Nullable
    public String user;

    @Name(Properties.PASSWORD)
    @Description("Password to use to connect to the specified database. Required for databases that " +
      "need authentication. Optional for databases that do not require authentication.")
    @Nullable
    public String password;

    @Name(Properties.AUTH_CONNECTION_STRING)
    @Nullable
    @Description("Auxiliary MongoDB connection string to authenticate against when constructing splits.")
    @Macro
    private String authConnectionString;

    @Name(Properties.CONNECTION_ARGUMENTS)
    @Description("A list of arbitrary string key/value pairs as connection arguments.")
    @Nullable
    public String connectionArguments;

    /**
     * @return {@link Schema} of the dataset if one was given else null
     * @throws IllegalArgumentException if the schema is null or not a valid JSON
     */
    public Schema getSchema() {
      if (schema == null) {
        throw new IllegalArgumentException("Schema cannot be null.");
      }
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format("Unable to parse schema '%s'. Reason: %s",
                                                         schema, e.getMessage()), e);
      }
    }

    public String getConnectionString() {
      StringBuilder connectionStringBuilder = new StringBuilder("mongodb://");
      if (!Strings.isNullOrEmpty(user) || !Strings.isNullOrEmpty(password)) {
        connectionStringBuilder.append(user).append(":").append(password).append("@");
      }
      connectionStringBuilder.append(host).append(":").append(port).append("/")
        .append(database).append(".").append(collection);

      if (!Strings.isNullOrEmpty(connectionArguments)) {
        connectionStringBuilder.append("?").append(connectionArguments);
      }

      return connectionStringBuilder.toString();
    }
  }

  /**
   * Property names for the config.
   */
  public static class Properties {
    public static final String AUTH_CONNECTION_STRING = "authConnectionString";
    public static final String CONNECTION_STRING = "connectionString";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String DATABASE = "database";
    public static final String COLLECTION = "collection";
    public static final String SCHEMA = "schema";
    public static final String INPUT_QUERY = "inputQuery";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String CONNECTION_ARGUMENTS = "connectionArguments";
    public static final String INPUT_FIELDS = "inputFields";
    public static final String SPLITTER_CLASS = "splitterClass";
  }
}
