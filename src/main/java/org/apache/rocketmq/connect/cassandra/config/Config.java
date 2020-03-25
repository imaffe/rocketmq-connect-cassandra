/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.connect.cassandra.config;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Config {
    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

    /* Database Connection Config */
    private String dbUrl;
    private String dbPort;
    private String dbUsername;
    private String dbPassword;
    private String dataType;
    private String rocketmqTopic;
    private String jdbcBackoff;
    private String jdbcAttempts;
    private String catalogPattern;
    private List tableWhitelist;
    private List tableBlacklist;
    private String schemaPattern;
    private boolean numericPrecisionMapping = false;
    private String bumericMapping;
    private String dialectName = "";
    private String whiteDataBase;
    private String whiteTable;

    public static final String CONN_TASK_PARALLELISM = "task-parallelism";
    public static final String CONN_TASK_DIVIDE_STRATEGY = "task-divide-strategy";
    public static final String CONN_WHITE_LIST = "whiteDataBase";
    public static final String CONN_SOURCE_RECORD_CONVERTER = "source-record-converter";
    public static final String CONN_DB_IP = "dbUrl";
    public static final String CONN_DB_PORT = "dbPort";
    public static final String CONN_DB_USERNAME = "dbUsername";
    public static final String CONN_DB_PASSWORD = "dbPassword";
    public static final String CONN_DATA_TYPE = "dataType";
    public static final String CONN_TOPIC_NAMES = "topicNames";
    public static final String CONN_DB_MODE = "mode";

    public static final String CONN_SOURCE_RMQ = "source-rocketmq";
    public static final String CONN_SOURCE_CLUSTER = "source-cluster";
    public static final String REFRESH_INTERVAL = "refresh.interval";

    /* Mode Config */
    private String mode = "";
    private String incrementingColumnName = "";
    private String query = "";
    private String timestampColmnName = "";
    private boolean validateNonNull = true;

    /*Connector config*/
    private String tableTypes = "table";
    private long pollInterval = 5000;
    private int batchMaxRows = 100;
    private long tablePollInterval = 60000;
    private long timestampDelayInterval = 0;
    private String dbTimezone = "GMT+8";
    private String queueName;

    private Logger log = LoggerFactory.getLogger(Config.class);
    public static final Set<String> REQUEST_CONFIG = new HashSet<String>() {
        {
            add("dbUrl");
            add("dbPort");
            add("dbUsername");
            add("dbPassword");
            add("mode");
            add("rocketmqTopic");
        }
    };

}