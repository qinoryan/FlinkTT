package Flink01.day01.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * TODO 基于：DateSet-API
 * TODO 批处理
 * @author User:qin
 * @create 2021-09-02 16:29
 */
public class BatWordCount {
    public static void main(String[] args) throws Exception {
        //获取入口
        //1.获取执行环境
        ExecutionEnvironment env
                = ExecutionEnvironment.getExecutionEnvironment();
        //2.从文件中读取数据
        DataSource<String> lineDs
                = env.readTextFile("input/words.txt");

        //3.转换数据格式，拆分数据(a,1)...
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne
                = lineDs.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }

        }).returns(Types.TUPLE(Types.STRING,Types.LONG));

        //4.按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOne.groupBy(0);
        //5.每个组内进行聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);
        //6.打印
        sum.print();
    }
}
