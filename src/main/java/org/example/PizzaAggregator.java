package org.example;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

class PizzaAggregator implements AggregateFunction<Tuple4<Integer, String, Integer, Long>, PizzaAggregateAccumulator, Tuple5<Integer, String, Integer, Long, Long>> {


    @Override
    public PizzaAggregateAccumulator createAccumulator() {
        return new PizzaAggregateAccumulator();
    }

    @Override
    public PizzaAggregateAccumulator add(Tuple4<Integer, String, Integer, Long> value, PizzaAggregateAccumulator accumulator) {
        accumulator.store = value.f0;
        accumulator.category = value.f1;
        accumulator.sum += value.f2;
        accumulator.earliest = Long.min(accumulator.earliest, value.f3);
        accumulator.latest = Long.max(accumulator.latest, value.f3);
        return accumulator;
    }

    @Override
    public Tuple5<Integer, String, Integer, Long, Long> getResult(PizzaAggregateAccumulator accumulator) {
//        System.out.println("Returning Tuple");
        return new Tuple5<>(accumulator.store, accumulator.category, accumulator.sum, accumulator.earliest, accumulator.latest);
    }

    @Override
    public PizzaAggregateAccumulator merge(PizzaAggregateAccumulator a, PizzaAggregateAccumulator b) {
        a.sum += b.sum;
        a.earliest = Long.min(a.earliest, b.earliest);
        a.latest = Long.max(a.latest, b.latest);
        return a;
    }
}
