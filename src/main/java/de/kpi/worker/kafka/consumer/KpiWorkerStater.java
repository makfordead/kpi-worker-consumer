package de.kpi.worker.kafka.consumer;

import io.smallrye.reactive.messaging.annotations.Incomings;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;


@Slf4j
@ApplicationScoped
public class KpiWorkerStater {

    @Incomings(value = {@Incoming("binance-data-feed"), @Incoming("bitmex-data-feed"), @Incoming("bitstamp-data-feed"),
            @Incoming("coinbase-data-feed"), @Incoming("kraken-data-feed")})
    public void consume(final List<Map<String, Object>> value) {
        value.forEach(v -> log.info("value: {}", v));
    }

}
