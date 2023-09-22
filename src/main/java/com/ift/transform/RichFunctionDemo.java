package com.ift.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liufei
 */
public class RichFunctionDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        final DataStreamSource<String> source = env.socketTextStream("192.168.3.24", 8888);
        final SingleOutputStreamOperator<Integer> streamOperator = source
//                .fromElements("1", "2", "3")
                /**
                 * RichXXXFunction：
                 * 1.多了生命周期管理方法：
                 *  open(): 在子任务启动时调用
                 *  close(): 在子任务结束时调用
                 * 2.多了上下文信息：
                 *  可以通过上下文对象获取到比如：子任务序号，子任务名称等信息
                 */
                .map(new RichMapFunction<String, Integer>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("子任务编号：" + getRuntimeContext().getIndexOfThisSubtask() + "，子任务名：" + getRuntimeContext().getTaskNameWithSubtasks());
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("子任务编号：" + getRuntimeContext().getIndexOfThisSubtask() + "，子任务名：" + getRuntimeContext().getTaskNameWithSubtasks());
                    }

                    @Override
                    public Integer map(String s) throws Exception {
                        return Integer.parseInt(s) + 1;
                    }
                });
        streamOperator.print();
        env.execute();
    }
}
