package org.example;

import io.confluent.developer.avro.order_line;
import io.confluent.developer.avro.pizza_orders;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
        List<order_line> lines = new ArrayList<>();
        order_line a =  new order_line(1, "pizza", 1, 1.0, 1.0);
        lines.add(a);
        fred.setOrderLines(lines);

        System.out.println(convertToJsonString(fred));

    }

    public static <T extends GenericRecord> String convertToJsonString(T event) throws IOException {

        String jsonstring;

        DatumWriter<T> writer = new GenericDatumWriter<>(event.getSchema());
        OutputStream out = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(event.getSchema(), out);
        writer.write(event, encoder);
        encoder.flush();
        jsonstring = out.toString();

        return jsonstring;
    }
}
