package de.kpi.worker.kafka.consumer.all.deserializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
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
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.TICKER;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.TS_LOCAL;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.TS_TICKER;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.TYPE;

@Slf4j
public class TickerDeserializer implements Deserializer<List<Map<String, Object>>> {

    final ObjectMapper mapper = new ObjectMapper();

    @Override
    public List<Map<String, Object>> deserialize(final String topic, final byte[] data) {
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            final Map<String, Object> map = mapper.readValue(data, new TypeReference<>() {
            });
            Optional.ofNullable(map.get(DATA)).filter(v -> v instanceof List).ifPresent(m -> ((List<Map<String, Object>>) m)
                    .forEach(a -> {
                        final Map<String, Object> mp = filterMap(a);
                        mp.put(MID, calculateMid(mp));
                        mp.put(EXCHANGE, topic);
                        list.add(mp);
                    }));
            Optional.ofNullable(map.get(DATA)).filter(v -> v instanceof Map).ifPresent(m -> {
                final Map<String, Object> mp = filterMap((Map<String, Object>) m);
                Optional.of((Map<String, Object>) m).filter(k -> k.containsKey(BIDS) && k.containsKey(ASKS)).ifPresentOrElse(v -> {
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
                }, () -> {
                    mp.put(MID, calculateMid(mp));
                    mp.put(EXCHANGE, topic);
                    list.add(mp);
                });
            });
            Optional.of(map).filter(k -> k.containsKey(TYPE) && TICKER.equals(k.get(TYPE))).ifPresent(m -> {
                final Map<String, Object> mp = filterMap(m);
                mp.put(MID, calculateMid(mp));
                mp.put(EXCHANGE, topic);
                list.add(mp);
            });
            Optional.of(map).filter(k -> !k.containsKey(DATA) && !k.containsKey(TYPE)).ifPresent(m -> {
                final Map<String, Object> mp = filterMap(m);
                mp.put(MID, calculateMid(mp));
                mp.put(EXCHANGE, topic);
                list.add(mp);
            });
        } catch (Exception ex) {
            log.error("Can't deserialize: ", ex);
        }
        return list;
    }

    private Map<String, Object> filterMap(final Map<String, Object> map) {
        final Map<String, Object> newMap = new HashMap<>();
        newMap.put(TS_LOCAL, LocalDateTime.now());
        keyMapping().keySet().forEach(k -> Optional.ofNullable(map.get(k)).ifPresent(v -> newMap.put(keyMapping().get(k), v)));
        return newMap;
    }

    private static double calculateMid(final Map<String, Object> mp) {
        return Double.sum(Double.parseDouble(mp.get(ASK).toString()), Double.parseDouble(mp.get(BID).toString())) / 2;
    }

    private static Map<String, String> keyMapping() {
        Map<String, String> map = new HashMap<>();
        map.put("askPrice", ASK);
        map.put("bidPrice", BID);
        map.put("timestamp", TS_TICKER);
        map.put("time", TS_TICKER);
        map.put("pair", SYMBOL);
        map.put("symbol", SYMBOL);
        map.put("ask", ASK);
        map.put("bid", BID);
        map.put("a", ASK);
        map.put("b", BID);
        map.put("T", TS_TICKER);
        map.put("s", SYMBOL);
        map.put("best_ask", ASK);
        map.put("best_bid", BID);
        map.put("product_id", SYMBOL);
        map.put("channel", SYMBOL);
        return map;
    }

}
