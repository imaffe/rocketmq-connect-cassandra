
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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.cassandra.common;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import java.net.InetSocketAddress;
import org.apache.rocketmq.connect.cassandra.config.Config;
import org.apache.rocketmq.connect.cassandra.connector.CassandraSinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class DBUtils {

    // TODO why do we have to use JdbcSourceTask as our main class
    private static final Logger log = LoggerFactory.getLogger(CassandraSinkTask.class);

    public static CqlSession initCqlSession(Config config) throws Exception {
        Map<String, String> map = new HashMap<>();

        // TODO Currently only support the simplest form

        CqlSessionBuilder sessionBuilder = CqlSession.builder();

        String contactPoint = config.getContactPoint(); // TODO can be extended to a list of contactPoint
        String contactPointPort = config.getContactPointPort();
        String localDataCenter = config.getLocalDataCenter();
        String username =  config.getDbUsername();
        String password =  config.getDbPassword();

        sessionBuilder.addContactPoint(new InetSocketAddress(contactPoint, Integer.parseInt(contactPoint)))
                      .withLocalDatacenter(localDataCenter)
                      .withAuthCredentials(username, password)
        ;


        // log.info("{} config read successful", map);
        CqlSession cqlSession = sessionBuilder.build();
        log.info("init Cql Session success");

        return cqlSession;
    }
}
