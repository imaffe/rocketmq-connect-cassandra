
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


package org.apache.rocketmq.connect.cassandra.sink;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.openmessaging.connector.api.data.EntryType;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import org.apache.rocketmq.connect.cassandra.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Updater {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Queue<CqlSession> connections = new ConcurrentLinkedQueue<>();
    private Config config;
    private CqlSession cqlSession;

    public Updater(Config config, CqlSession cqlSession) {
        this.config = config;
        this.cqlSession = cqlSession;
    }


    public boolean push(String dbName, String tableName, Map<Field, Object[]> fieldMap, EntryType entryType) {
        Boolean isSuccess = false;
        int beforeUpdateId = 0;
        int afterUpdateId = 0;
        switch (entryType) {
            case CREATE:
                // TODO : looks like it's handling Duplicated Message
                // TODO should check if record already present in CQL
//                afterUpdateId = queryAfterUpdateRowId(dbName, tableName, fieldMap);
//                if (afterUpdateId != 0){
//                    isSuccess = true;
//                    break;
//                }
                isSuccess = updateRow(dbName, tableName, fieldMap, beforeUpdateId);
                break;
            case UPDATE:
                log.info("UPDATE not supported yet");
                isSuccess = true;
                break;
            case DELETE:
                log.info("DELETE not supported yet");
                isSuccess = true;
                break;
            default:
                log.error("entryType {} is illegal.", entryType.toString());
        }
        return isSuccess;
    }

    public void start() throws Exception {
        log.info("schema load success");
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    private String typeParser(FieldType fieldType, String fieldName, Object fieldValue, String sql) {
        switch (fieldType) {
            case STRING:
                sql += fieldName + " = " + "'" + fieldValue + "'";
                break;
            case DATETIME:
                log.info("DATETIME type not supported yet");
                break;
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BIG_INTEGER:
                sql += fieldName + " = " + fieldValue;
                break;
            default:
                log.error("fieldType {} is illegal.", fieldType.toString());
        }
        return sql;
    }

    private Boolean updateRow(String dbName, String tableName, Map<Field, Object[]> fieldMap, Integer id) {
        int count = 0;
        SimpleStatement stmt;
        boolean finishUpdate = false;
        String update = "update " + dbName + "." + tableName + " set ";

        for (Map.Entry<Field, Object[]> entry : fieldMap.entrySet()) {
            count++;
            String fieldName = entry.getKey().getName();
            FieldType fieldType = entry.getKey().getType();
            Object fieldValue = entry.getValue()[1];
//            if ("id".equals(fieldName)) {
//                // TODO should not be executed as there is no default id in canssandra
//                if (id == 0)
//                    continue;
//                else
//                    fieldValue = id;
//            }
            if (count != 1) {
                update += ", ";
            }
            if (fieldValue == null) {
                update += fieldName + " = NULL";
            } else {
                update = typeParser(fieldType, fieldName, fieldValue, update);
            }
        }

        try {
            while (!cqlSession.isClosed() && !finishUpdate){
                stmt = SimpleStatement.newInstance(update);
                ResultSet result = cqlSession.execute(stmt);
                if (result.wasApplied()) {
                    log.info("replace into table success");
                    return true;
                }
                finishUpdate = true;
            }
        } catch (Exception e) {
            log.error("update table error,{}", e);
        }
        return false;
    }
}
