package de.kpi.worker.kafka.consumer.coinbase;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;


@Slf4j
@ApplicationScoped
public class CoinbaseStater {

    @Incoming("coinbase-data-feed")
    public void consume(final List<Map<String, Object>> value) {
        value.forEach(v -> log.info("value: {}", v));
    }

}
