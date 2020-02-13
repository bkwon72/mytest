/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.jdbc.topology;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.spout.UserSpout;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class AbstractUserTopology {
    private static final List<String> setupSqls = Lists.newArrayList(
            "USE STORM_JDBC",
            "drop table if exists user",
            "drop table if exists department",
            "drop table if exists user_department",
            "create table if not exists user (seq_num integer not null, user_id integer, user_name varchar(100), dept_name varchar(100), create_date datetime)",
            "create table if not exists department (dept_id integer, dept_name varchar(100))",
            "create table if not exists user_department (user_id integer, dept_id integer)",
            "insert into department values (1, 'R&D')",
            "insert into department values (2, 'Finance')",
            "insert into department values (3, 'HR')",
            "insert into department values (4, 'Sales')",
            "insert into user_department values (1, 1)",
            "insert into user_department values (2, 2)",
            "insert into user_department values (3, 3)",
            "insert into user_department values (4, 4)"
    );
    protected UserSpout userSpout;
    protected JdbcMapper jdbcMapper;
    protected JdbcLookupMapper jdbcLookupMapper;
    protected ConnectionProvider connectionProvider;

    protected static final String TABLE_NAME = "user";
    protected static final String JDBC_CONF = "jdbc.conf";
    protected static final String SELECT_QUERY =
            "select dept_name from department, user_department where department.dept_id = user_department.dept_id" +
                    " and user_department.user_id = ?";

    public void execute(String[] args) throws Exception {
        if (args != null) {
            List<String> alist = Arrays.asList(args);
            alist.forEach(System.out::println);
        }

        if (args.length != 3 && args.length != 4) {
            System.out.println("Usage: " + this.getClass().getSimpleName() + " <dataSource.url> "
                    + "<user> <password> [topology name]");
            System.exit(-1);
        }

        String topologyName = null;
        if (args.length == 4) {
            topologyName = args[3];
        }

        Map<String, Object> map = initDBSchema(args);
        Config config = new Config();
        config.put(JDBC_CONF, map);

        this.userSpout = new UserSpout();
        this.connectionProvider = new HikariCPConnectionProvider(map);
        if (topologyName == null) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, getTopology());
            Thread.sleep(3000);
            cluster.killTopology("test");
            cluster.shutdown();
            System.exit(0);
        } else {
            StormSubmitter.submitTopology(topologyName, config, getTopology());
        }
    }

    private Map<String, Object> initDBSchema(String[] args) {
        Map<String, Object> map = Maps.newHashMap();
        map.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        map.put("dataSource.url", args[0]);//jdbc:mysql://localhost/test
        map.put("dataSource.user", args[1]);//root
        map.put("dataSource.password", args[2]);//password

        // connection 생성
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(map);
        connectionProvider.prepare();

        // 초기값 셋팅 query 실행
        JdbcClient jdbcClient = new JdbcClient(connectionProvider, 60);
        for (String sql : setupSqls) {
            jdbcClient.executeSql(sql);
        }
        // db connection close
        connectionProvider.cleanup();
        return map;
    }

    public abstract StormTopology getTopology();

}
