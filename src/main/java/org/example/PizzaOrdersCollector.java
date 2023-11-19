package org.example;


import io.confluent.developer.avro.order_line;
import io.confluent.developer.avro.pizza_orders;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import org.apache.avro.Schema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class PizzaOrdersCollector {

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
                .setGroupId("Corans-Flink-Consumer-for-Pizza-Collector")
                .setTopics("pizza_orders_32")
                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setDeserializer(new PizzaOrdersDeserializer())
                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forSpecific(pizza_orders.class, kafkaConnectionProperties.getProperty("schema.registry.url"), 1000, registryConfigs));
//                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forGeneric(Pizza_Orders.getClassSchema(), kafkaConnectionProperties.getProperty("schema.registry.url"), 1000, registryConfigs));

        KafkaSource<pizza_orders> kafkaSource = kafkaSourceBuilder.build();

        DataStream<pizza_orders> pizzaOrdersDataStream = env.fromSource(kafkaSource, WatermarkStrategy.<pizza_orders>forMonotonousTimestamps().withIdleness(Duration.ofSeconds(1)), "Kafka Source");

        DataStream<String> jsonStream  = pizzaOrdersDataStream.process(new Pizza2json());
        jsonStream.print();

        env.execute();
    }

    private static class Pizza2json extends ProcessFunction<pizza_orders, String> {


        @Override
        public void processElement(pizza_orders value, ProcessFunction<pizza_orders, String>.Context ctx, Collector<String> out) throws Exception {

            JSONObject jpo = new JSONObject();
            List<Schema.Field> fields = value.getSchema().getFields();
            for (Schema.Field f : fields) {
                if (f.pos()==5) {
                    List<order_line> l = (List<order_line>) value.get(f.pos());
                    JSONArray ja = new JSONArray();
                    for (order_line o: l) {
                        JSONObject jo = new JSONObject();
                        for (Schema.Field g : o.getSchema().getFields()) {
                            jo.put(g.name(), o.get(g.pos()));
                        }
                        ja.put(jo);
                    }
                } else jpo.append(f.name(), value.get(f.pos()));
            }

            out.collect(jpo.toString());
        }
    }
}


