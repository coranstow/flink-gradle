package org.example;

import io.confluent.developer.avro.order_line;
import io.confluent.developer.avro.pizza_orders;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class testjson {

    public static void main(String[] args) throws IOException {
        pizza_orders fred = new pizza_orders();
//        fred.setDate(LocalDate.now());
        fred.setCouponCode(1);
        fred.setStatus("Ordered");
        fred.setStoreId(1);
        fred.setStoreOrderId(1);
        List<order_line> lines = new ArrayList<order_line>();
        order_line a =  new order_line(1, "pizza", 1, 1.0, 1.0);
        lines.add(a);
        fred.setOrderLines(lines);

        System.out.println(convertToJsonString(fred));

    }

    private abstract static class IgnoreSchemaProperty {
        @JsonIgnore abstract void getSchema();
    }

    public static <T extends GenericRecord> String convertToJsonString(T event) throws IOException {

        String jsonstring = "";

        try {
            DatumWriter<T> writer = new GenericDatumWriter<T>(event.getSchema());
            OutputStream out = new ByteArrayOutputStream();
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(event.getSchema(), out);
            writer.write(event, encoder);
            encoder.flush();
            jsonstring = out.toString();
        } catch (IOException e) {
            throw e;
        }

        return jsonstring;
    }
}
