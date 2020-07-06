package de.kpi.worker.kafka.consumer.binance;

import de.kpi.worker.kafka.consumer.deserializer.KafkaMessage;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;


@Slf4j
@ApplicationScoped
public class BinanceStater {

    @Incoming("binance-data-feed")
    public void consume(final List<Map<String, Object>> value) {
        value.forEach(v -> log.info("value: {}", v));
    }

}
