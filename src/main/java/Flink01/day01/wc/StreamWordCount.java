package Flink01.day01.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TODO 流处理
 *
 * @author User:qin
 * @create 2021-09-02 16:55
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取文件
        DataStreamSource<String> lineStream = env.readTextFile("input/words.txt");
//        // 读取文本流
//        DataStreamSource<String> lineStream = env.socketTextStream("hadoop102", 7777)

        //3.分词转换并分组统计
        SingleOutputStreamOperator<Tuple2<String, Long>> sum
                = lineStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0) //TODO 直接调用keyBy方法参数使用lambda表达式
                .sum(1);
        //4.打印输出
        //TODO 此打印会将中间结果一一输出出来 --流处理
        // ( ?> ()  ? 类似表示并行线程编号 )
        sum.print();
        //5.执行
        env.execute();
    }
}
