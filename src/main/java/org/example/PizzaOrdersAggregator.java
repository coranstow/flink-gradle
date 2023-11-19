package org.example;

import io.confluent.developer.avro.pizza_orders;
import io.confluent.developer.avro.order_line;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

public class PizzaOrdersAggregator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaConnectionProperties = new Properties();
        kafkaConnectionProperties.load(PizzaOrdersAggregator.class.getClassLoader().getResourceAsStream("confluentcloud.properties"));
//        kafkaConnectionProperties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        Map<String, String> registryConfigs = new HashMap<>();
        registryConfigs.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, kafkaConnectionProperties.getProperty(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE));
        registryConfigs.put(SchemaRegistryClientConfig.USER_INFO_CONFIG, kafkaConnectionProperties.getProperty(SchemaRegistryClientConfig.USER_INFO_CONFIG));

        KafkaSourceBuilder<pizza_orders> kafkaSourceBuilder = KafkaSource.<pizza_orders>builder()
                .setProperties(kafkaConnectionProperties)
                .setGroupId("Corans-Flink-Consumer-for-Pizza-Orders")
                .setTopics("pizza_orders_32")
                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setDeserializer(new PizzaOrdersDeserializer())
                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forSpecific(pizza_orders.class, kafkaConnectionProperties.getProperty("schema.registry.url"), 1000, registryConfigs));
//                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forGeneric(Pizza_Orders.getClassSchema(), kafkaConnectionProperties.getProperty("schema.registry.url"), 1000, registryConfigs));

        KafkaSource<pizza_orders> kafkaSource = kafkaSourceBuilder.build();

        DataStream<pizza_orders> pizzaOrdersDataStream = env.fromSource(kafkaSource, WatermarkStrategy.<pizza_orders>forMonotonousTimestamps().withIdleness(Duration.ofSeconds(1)), "Kafka Source");

        DataStream<Tuple2<Integer, order_line>> lineItemsDataStream = pizzaOrdersDataStream.flatMap(new PizzaOrdersTupleExtractor()).name("FlatMap orders to line items");

        DataStream<Tuple3<Integer, String, Integer>> lineItemTuplesDataStream = lineItemsDataStream.map(new unwrapLineOrder()).name("Unwrap Line Items");
        // lineItemsDataStream.print();


        DataStream<Tuple4<Integer, String, Integer, Long>> timestampedLineItemTuplesDataStream = lineItemTuplesDataStream.process(new addTimeStampToData()).name("Get Timestamp from metadata");
//        timestampedLineItemTuplesDataStream.print();

        KeyedStream<Tuple4<Integer, String, Integer, Long>, Tuple2<Integer, String>> timestampedLineItemTuplesDataStreamKeyedOnIDAndCategory = timestampedLineItemTuplesDataStream.keyBy(new getKeyAsStoreIDAndCategory());

        SingleOutputStreamOperator<Tuple5<Integer, String, Integer, String, String>> lineItems1HourAggregate = timestampedLineItemTuplesDataStreamKeyedOnIDAndCategory.
                window(TumblingEventTimeWindows.of(Time.hours(1))).aggregate(new PizzaAggregator())
                .name("Aggregate line items by store and category").map(new getTuple5MapDatesToStrings()).name("Convert Dates to String representation");

        DataStreamSink<Tuple5<Integer, String, Integer, String, String>> csvSink = lineItems1HourAggregate.writeAsText("output.txt", FileSystem.WriteMode.OVERWRITE).name("Write as text").setParallelism(1);

        // Chained version of the above
//        DataStreamSink<Tuple5<Integer, String, Integer, String, String>> csvSink = pizzaOrdersDataStream
//                .flatMap(new PizzaOrdersTupleExtractor()).name("Map orders to line items")
//                .map(new unwrapLineOrder()).name("Unwrap Line Items")
//                .process(new addTimeStampToData()).name("Get Timestamp from metadata")
//                .keyBy(new getKeyAsStoreIDAndCategory())
//                .window(TumblingEventTimeWindows.of(Time.hours(1))).aggregate(new PizzaAggregator()).name("Aggregate line items by store and category")
//                .map(new getTuple5MapDatesToStrings()).name("Convert Dates to String representation")
//                .writeAsText("output.txt", FileSystem.WriteMode.OVERWRITE).name("Write as text").setParallelism(1);


//        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        aggregated.print();


        env.execute();

    }

    private static class getTuple5MapDatesToStrings implements MapFunction<Tuple5<Integer, String, Integer, Long, Long>, Tuple5<Integer, String, Integer, String, String>>{
        @Override
        public Tuple5<Integer, String, Integer, String, String> map(Tuple5<Integer, String, Integer, Long, Long> value) {
            return new Tuple5<>(value.f0, value.f1, value.f2, new Date(value.f3).toString(), new Date(value.f4).toString());
        }
    }

    private static class unwrapLineOrder implements MapFunction<Tuple2<Integer, order_line>, Tuple3<Integer, String, Integer>> {

        @Override
        public Tuple3<Integer, String, Integer> map(Tuple2<Integer, order_line> value)  {
            return new Tuple3<>(value.f0, value.f1.getCategory(), value.f1.getQuantity());
        }
    }

    private static class getKeyAsStoreIDAndCategory implements KeySelector<Tuple4<Integer, String, Integer, Long>, Tuple2<Integer, String>> {
        @Override
        public Tuple2<Integer, String> getKey(Tuple4<Integer, String, Integer, Long> value)  {
            return new Tuple2<>(value.f0, value.f1);
        }
    }
    private static class PizzaOrdersTupleExtractor implements FlatMapFunction<pizza_orders, Tuple2<Integer, order_line>> {
        public void flatMap(pizza_orders value, Collector<Tuple2<Integer, order_line>> out) {
            Integer storeId = value.getStoreId();
            List<order_line> orderLineList = value.getOrderLines();
            for (order_line orderLine : orderLineList) {
                out.collect(new Tuple2<>(storeId, orderLine));
                out.collect(new Tuple2<>(99, orderLine));
            }
        }
    }
//    private static class GenericRecordPizzaOrdersTupleExtractor implements FlatMapFunction<GenericRecord, Tuple2<Integer, order_line>> {
//        public void flatMap(GenericRecord value, Collector<Tuple2<Integer, order_line>> out)  {
//
//
//            Integer storeId = (Integer) value.get("store_id");
//            if (value.get("order_lines") instanceof List ) {
//                @SuppressWarnings("rawtypes")
//                List orderLineList = (List) value.get("order_lines");
//                for (GenericData.Record orderLine : orderLineList) {
//                    order_line toReturn = new order_line();
//                    toReturn.setCategory(orderLine.get);
////                    if (orderLine instanceof order_line) {
//                        out.collect(new Tuple2<>(storeId,  orderLine));
//                        out.collect(new Tuple2<>(99, (order_line) orderLine));
////                    } else System.out.println("Found List but order_line is actually" + orderLine.getClass().toString() + " and value is " + orderLine.toString());
//
//                }
//            }
//        }
//
//    }
    private static class addTimeStampToData extends ProcessFunction<Tuple3<Integer, String, Integer>, Tuple4<Integer, String, Integer, Long>> {
        @Override
        public void processElement(Tuple3<Integer, String, Integer> value, ProcessFunction<Tuple3<Integer, String, Integer>, Tuple4<Integer, String, Integer, Long>>.Context ctx, Collector<Tuple4<Integer, String, Integer, Long>> out) {
            out.collect(new Tuple4<>(value.f0, value.f1, value.f2, ctx.timestamp()));
//            System.out.println(value.toString());
        }
    }


}