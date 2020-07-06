package de.kpi.worker.kafka.consumer.bitstamp;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;


@Slf4j
@ApplicationScoped
public class BitstampStater {

    @Incoming("bitstamp-data-feed")
    public void consume(final String value) {
        log.info("value: {}", value);
    }

}
