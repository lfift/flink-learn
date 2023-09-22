package com.ift.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liufei
 */
public class FileSourceDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("files/flink.txt")).build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource").print();
        env.execute();
    }
}
