package de.kpi.worker.kafka.consumer.bitstamp.deserializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.ASK;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.ASKS;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.BID;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.BIDS;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.CHANNEL;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.DATA;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.EXCHANGE;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.MID;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.SYMBOL;
import static de.kpi.worker.kafka.consumer.util.JsonKeyUtils.calculateMid;
import static de.kpi.worker.kafka.consumer.util.JsonKeyUtils.filterMap;

@Slf4j
public class BitstampDeserializer implements Deserializer<List<Map<String, Object>>> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public List<Map<String, Object>> deserialize(final String topic, final byte[] data) {
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            final Map<String, Object> map = mapper.readValue(data, new TypeReference<>() {
            });
            Optional.ofNullable(map.get(DATA)).filter(v -> v instanceof Map).ifPresent(m -> {
                final Map<String, Object> mp = filterMap((Map<String, Object>) m);
                Optional.of((Map<String, Object>) m).filter(k -> k.containsKey(BIDS) && k.containsKey(ASKS)).ifPresent(v -> {
                    final List<List<String>> bids = (List<List<String>>) v.get(BIDS);
                    final List<List<String>> asks = (List<List<String>>) v.get(ASKS);
                    IntStream.range(0, bids.size()).forEach(i -> {
                        final List<String> b = bids.get(i);
                        final List<String> a = asks.get(i);
                        mp.put(BID, b.get(0));
                        mp.put(ASK, a.get(0));
                        mp.put(MID, calculateMid(mp));
                        mp.put(SYMBOL, map.get(CHANNEL));
                        mp.put(EXCHANGE, topic);
                        list.add(mp);
                    });
                });
            });
        } catch (Exception ex) {
            log.error("Can't deserialize: ", ex);
        }
        return list;
    }
}
