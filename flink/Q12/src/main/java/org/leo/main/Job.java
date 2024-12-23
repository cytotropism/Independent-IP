package org.leo.main;

import org.leo.RelationType.Payload;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

public class Job {
    public static final OutputTag<Payload> lineitemTag = new OutputTag<Payload>("lineitem"){};
    public static final OutputTag<Payload> ordersTag = new OutputTag<Payload>("orders"){};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String inputPath = parameterTool.get("input");
        String outputPath = parameterTool.get("output");

        DataStreamSource<String> data = env.readTextFile(inputPath).setParallelism(1);
        SingleOutputStreamOperator<Payload> inputStream = getStream(data);
        
        DataStream<Payload> orders = inputStream.getSideOutput(ordersTag);
        DataStream<Payload> lineitem = inputStream.getSideOutput(lineitemTag);

        DataStream<Payload> ordersS = orders.keyBy(i -> i.key)
                .process(new OrdersProcessFunction());
        DataStream<Payload> lineitemS = lineitem.keyBy(i -> i.key)
                .process(new LineitemProcessFunction());
        
        DataStream<Payload> result = ordersS.connect(lineitemS)
                .keyBy(i -> i.key, i -> i.key)
                .process(new NewAggregateProcessFunction());

        DataStreamSink<Payload> output = result.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute("Flink Streaming Java API for New Query");
    }

    private static SingleOutputStreamOperator<Payload> getStream(DataStreamSource<String> data) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        return data.process(new ProcessFunction<String, Payload>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Payload> out) throws Exception {
                String header = value.substring(0, 3);
                String[] cells = value.substring(3).split("\\|");
                String action = "";
                switch (header) {
                    case "+LI":
                        action = "Insert";
                        ctx.output(lineitemTag, new Payload(action, Long.valueOf(cells[0]),
                                new ArrayList<>(Arrays.asList("L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_ORDERKEY", "L_QUANTITY", "L_EXTENDEDPRICE")),
                                new ArrayList<>(Arrays.asList(format.parse(cells[1]), format.parse(cells[2]), format.parse(cells[3]), Long.valueOf(cells[4]), Integer.valueOf(cells[5]), Double.valueOf(cells[6]))));
                        break;
                    case "+OR":
                        action = "Insert";
                        ctx.output(ordersTag, new Payload(action, Long.valueOf(cells[1]),
                                new ArrayList<>(Arrays.asList("O_ORDERKEY", "O_ORDERPRIORITY")),
                                new ArrayList<>(Arrays.asList(Long.valueOf(cells[1]), cells[2]))));
                        break;
                    // Handle Deletes if necessary
                }
            }
        }).setParallelism(1);
    }
}