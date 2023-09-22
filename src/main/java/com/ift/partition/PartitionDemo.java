package com.ift.partition;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liufei
 */
public class PartitionDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        env.socketTextStream("192.168.3.24", 8888)
                //随机分区
                .shuffle()
                //轮询分区，如果是数据倾斜的场景，source进来后调用rebalance就可以解决数据倾斜问题
//                .rebalance()
                //缩放，实现轮询，局部组队，比rebalance更高效
//                .rescale()
                //广播，发送给下游的所有子任务
//                .broadcast()
                //全局分区（谨慎使用），所有source来源全部分给下游第一个子任务，下游子任务并行度强行为1
//                .global()
                //按指定key分区，相同key的数据发往同一个子任务
//                .keyBy()
                //one-to-one：forward分区器
//                .forward()
                .print();
        env.execute();

    }
}
