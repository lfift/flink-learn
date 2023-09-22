package com.ift.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.nio.charset.StandardCharsets;

/**
 * @author liufei
 */
public class KafkaSinkDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        //开起checkPoint并使用精准一次模式
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);

        final DataStreamSource<String> socketSource = env.socketTextStream("192.168.3.30", 9999);

        socketSource.sinkTo(KafkaSink.<String>builder()
                .setBootstrapServers("192.168.3.30:9092,192.168.3.31:9092,192.168.3.32:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("kafka-sink")
                        .setValueSerializationSchema(new SimpleStringSchema(StandardCharsets.UTF_8))
                        .build())
                //设置精准一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //如果是精准一次则必须设置事务Id前缀
                .setTransactionalIdPrefix("kafka-sink-tx-")
                //如果是精准一次必须设置事务超时时间且 checkPint时间 < 超时时间 < 最大超时时间（15分钟）
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(1000 * 10))
                .build());

        env.execute();


    }
}
