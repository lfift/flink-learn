package com.ift.sink;

import com.ift.bean.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author liufei
 */
public class MySqlSink {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<String> socketSource = env.socketTextStream("192.168.3.24", 9999);

        final SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink(
                "insert into ws values(?, ?, ?)",
                (ps, waterSensor) -> {
                    ps.setString(1, waterSensor.getId());
                    ps.setLong(2, waterSensor.getTs());
                    ps.setInt(3, waterSensor.getVc());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(10)
                        .withBatchIntervalMs(3000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.3.24:3306/flink?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
                        .withUsername("root")
                        .withPassword("0420lyf.")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        );

        socketSource.map(value -> {
            final String[] values = value.split(",");
            return new WaterSensor(values[0], Long.valueOf(values[1]), Integer.valueOf(values[2]));
        }).addSink(jdbcSink);

        env.execute();
    }
}
