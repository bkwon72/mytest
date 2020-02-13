package org.apache.storm.jdbc.bolt;

import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SeqJdbcLookupBolt extends JdbcLookupBolt {

    private static final Logger log = LoggerFactory.getLogger(SeqJdbcLookupBolt.class);
    private String selectQuery;
    private JdbcLookupMapper jdbcLookupMapper;

    public SeqJdbcLookupBolt(ConnectionProvider connectionProvider, String selectQuery, JdbcLookupMapper jdbcLookupMapper) {
        super(connectionProvider, selectQuery, jdbcLookupMapper);
        this.selectQuery = selectQuery;
        this.jdbcLookupMapper = jdbcLookupMapper;
    }

    private java.util.concurrent.atomic.AtomicInteger count = new AtomicInteger(0);

    @Override
    protected void process(Tuple tuple) {
        try {
            List<Column> columns = this.jdbcLookupMapper.getColumns(tuple);
            //columns.forEach(c -> log.info("colum = " + c.toString()));

            List<List<Column>> result = jdbcClient.select(this.selectQuery, columns);

            if (result != null && result.size() != 0) {
                for (List<Column> row : result) {
                    List<Values> values = jdbcLookupMapper.toTuple(tuple, row);
                    for (Values value : values) {
                        value.add(count.getAndIncrement());
                        //log.info("tuple = " + tuple + ", value = " + value.toString());
                        log.info("value = " + value.toString());
                        collector.emit(tuple, value);
                    }
                }
            }
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }
}
