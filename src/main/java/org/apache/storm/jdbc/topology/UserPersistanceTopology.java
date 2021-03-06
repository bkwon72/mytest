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
import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.bolt.SeqJdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.sql.Types;
import java.util.List;


public class UserPersistanceTopology extends AbstractUserTopology {
    private static final String USER_SPOUT = "USER_SPOUT";
    private static final String LOOKUP_BOLT = "LOOKUP_BOLT";
    private static final String PERSISTANCE_BOLT = "PERSISTANCE_BOLT";

    public static void main(String[] args) throws Exception {
        new UserPersistanceTopology().execute(args);
    }

    @Override
    public StormTopology getTopology() {
        Fields outputFields = new Fields("user_id", "user_name", "dept_name", "create_date","seq_num");
        List<Column> queryParamColumns = Lists.newArrayList(new Column("user_id", Types.INTEGER));
        this.jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
        SeqJdbcLookupBolt selectDepartmentBolt = new SeqJdbcLookupBolt(connectionProvider, SELECT_QUERY, this.jdbcLookupMapper);

        //must specify column schema when providing custom query.
        List<Column> schemaColumns = Lists.newArrayList(
                new Column("seq_num", Types.INTEGER),
                new Column("create_date", Types.TIMESTAMP),
                new Column("dept_name", Types.VARCHAR),
                new Column("user_id", Types.INTEGER),
                new Column("user_name", Types.VARCHAR));
        String insertSql = "insert into user (seq_num, create_date, dept_name, user_id, user_name) values (?,?,?,?,?)";
        JdbcInsertBolt userInsertBolt = new JdbcInsertBolt(connectionProvider, new SimpleJdbcMapper(schemaColumns))
                .withInsertQuery(insertSql);

        // userSpout ==> jdbcBolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(USER_SPOUT, this.userSpout, 1);
        builder.setBolt(LOOKUP_BOLT, selectDepartmentBolt, 1).shuffleGrouping(USER_SPOUT);
        builder.setBolt(PERSISTANCE_BOLT, userInsertBolt, 1).shuffleGrouping(LOOKUP_BOLT);
        return builder.createTopology();
    }
}
