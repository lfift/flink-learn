package com.ift.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;
import java.util.UUID;

/**
 * @author liufei
 */
public class FileSinkDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //同时有并行度数个文件写入
        env.setParallelism(2);
        //必须开起checkpoint否则文件一直处于.inprogress
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);

        final DataGeneratorSource<String> genSource = new DataGeneratorSource<>(v -> "Number-" + UUID.randomUUID(), Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(500), Types.STRING);

        env.fromSource(genSource, WatermarkStrategy.noWatermarks(), "random-str")
                .sinkTo(FileSink.<String>forRowFormat(new Path("e://tmp"), new SimpleStringEncoder<>("utf-8"))
                        //文件类型配置
                        .withOutputFileConfig(new OutputFileConfig("sink", ".log"))
                        //按日期分桶（目录）
                        .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                        //滚动策略检查频率
                        .withBucketCheckInterval(1L)
                        //滚动策略 10秒或1M
                        .withRollingPolicy(DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(new MemorySize(1024 * 1024)).build())
                        .build());
        env.execute();

    }
}
