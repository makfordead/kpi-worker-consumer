package de.kpi.worker.kafka.consumer.deserializer;

import lombok.Data;

@Data
public class KafkaMessage {

    String tsTicker;

    String tsLocal;

    String exchange;

    String symbol;

    double bid;

    double ask;

    double mid;


}
