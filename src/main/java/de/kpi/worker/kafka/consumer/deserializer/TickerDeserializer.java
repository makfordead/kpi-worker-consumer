package de.kpi.worker.kafka.consumer.deserializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class TickerDeserializer implements Deserializer<List<Map<String, Object>>> {

    final ObjectMapper mapper = new ObjectMapper();

    @Override
    public List<Map<String, Object>> deserialize(final String topic, final byte[] data) {
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            final Map<String, Object> map = mapper.readValue(data, new TypeReference<>() {
            });
            Optional.ofNullable(map.get("data")).filter(v -> v instanceof List).ifPresent(m -> ((List<Map<String, Object>>) m)
                    .forEach(a -> {
                        final Map<String, Object> mp = filterMap(a);
                        mp.put("mid", calculateMid(mp));
                        mp.put("exchange", topic);
                        list.add(mp);
                    }));
            Optional.ofNullable(map.get("data")).filter(v -> v instanceof Map).ifPresent(m -> {
                final Map<String, Object> mp = filterMap((Map<String, Object>) m);
                mp.put("mid", calculateMid(mp));
                mp.put("exchange", topic);
                list.add(mp);
            });
            Optional.of(map).filter(k -> !k.containsKey("data")).ifPresent(m -> {
                final Map<String, Object> mp = filterMap(m);
                mp.put("mid", calculateMid(mp));
                mp.put("exchange", topic);
                list.add(mp);
            });
        } catch (Exception ex) {
            log.error("Can't deserialize: ", ex);
        }
        return list;
    }

    private Map<String, Object> filterMap(final Map<String, Object> map) {
        final Map<String, Object> newMap = new HashMap<>();
        keyMapping().keySet().forEach(k -> Optional.ofNullable(map.get(k)).ifPresent(v -> newMap.put(keyMapping().get(k), v)));
        return newMap;
    }

    private static double calculateMid(final Map<String, Object> mp) {
        return Double.sum(Double.parseDouble(mp.get("ask").toString()), Double.parseDouble(mp.get("bid").toString())) / 2;
    }

    private static Map<String, String> keyMapping() {
        Map<String, String> map = new HashMap<>();
        map.put("askPrice", "ask");
        map.put("bidPrice", "bid");
        map.put("timestamp", "ts_ticker");
        map.put("time", "ts_ticker");
        map.put("pair", "symbol");
        map.put("symbol", "symbol");
        map.put("ask", "ask");
        map.put("bid", "bid");
        map.put("a", "ask");
        map.put("b", "bid");
        map.put("T", "ts_ticker");
        map.put("s", "symbol");
        return map;
    }

}
