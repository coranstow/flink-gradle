package org.example;

public class PizzaAggregateAccumulator {
    Integer store;
    String category;
    Long earliest;
    Long latest;
    Integer sum;

    public PizzaAggregateAccumulator() {
        this.sum = 0;
        this.earliest = Long.MAX_VALUE;
        this.latest = Long.MIN_VALUE;
    }
}
