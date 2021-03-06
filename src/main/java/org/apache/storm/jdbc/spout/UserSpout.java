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
package org.apache.storm.jdbc.spout;

import com.google.common.collect.Lists;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class UserSpout implements IRichSpout {
    public static final Logger log = LoggerFactory.getLogger(UserSpout.class);

    boolean isDistributed;
    SpoutOutputCollector collector;
    public static final List<Values> rows = Lists.newArrayList(
            new Values(1, "peter", Long.valueOf(System.currentTimeMillis())),
            new Values(2, "bob", Long.valueOf(System.currentTimeMillis())),
            new Values(3, "alice", Long.valueOf(System.currentTimeMillis())));

    public UserSpout() {
        this(true);
    }

    public UserSpout(boolean isDistributed) {
        this.isDistributed = isDistributed;
    }

    public boolean isDistributed() {
        return this.isDistributed;
    }

    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void close() {

    }

    public void nextTuple() {

        final Random rand = new Random();
        final Values row = rows.get(rand.nextInt(rows.size() - 1));
        Long nowLong = Long.valueOf(System.currentTimeMillis());
        row.set(2, nowLong);

        this.collector.emit(row);
         Thread.yield();
    }

    public void ack(Object msgId) {
        log.error(msgId.toString());
    }

    public void fail(Object msgId) {
        log.error(msgId.toString());
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user_id", "user_name", "create_date"));
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
