package de.kpi.worker.kafka.consumer.coinbase;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;


@Slf4j
@ApplicationScoped
public class CoinbaseStater {

    @Incoming("coinbase-data-feed")
    public void consume(final String value) {
        log.info(value);
    }

}
