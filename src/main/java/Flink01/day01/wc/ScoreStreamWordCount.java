package Flink01.day01.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TODO 流处理
 * @author User:qin
 * @create 2021-09-02 17:15
 */
public class ScoreStreamWordCount {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取文件/文本流
        DataStreamSource<String> SourceStream = environment.socketTextStream("hadoop102", 6666);

        //3.分词转换并分组统计
        SourceStream.flatMap((String inline, Collector<Tuple2<String, Long>> out) -> {
            String[] s = inline.split(" ");
            for (String s1 : s) {
                out.collect(Tuple2.of(s1, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(line -> line.f0) //TODO 分组方法参数为匿名
                .sum(1) //TODO 获取sum
                .print(); //TODO 打印

        //4.执行
        environment.execute();
        //sq

    }
}
