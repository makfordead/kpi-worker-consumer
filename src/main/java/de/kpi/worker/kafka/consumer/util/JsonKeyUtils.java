package de.kpi.worker.kafka.consumer.util;

import lombok.experimental.UtilityClass;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.ASK;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.BID;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.SYMBOL;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.TS_LOCAL;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.TS_TICKER;

@UtilityClass
public class JsonKeyUtils {


    public static Map<String, String> jsonKeyMapping() {
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

    public static double calculateMid(final Map<String, Object> mp) {
        return Double.sum(Double.parseDouble(mp.get(ASK).toString()), Double.parseDouble(mp.get(BID).toString())) / 2;
    }

    public static Map<String, Object> filterMap(final Map<String, Object> map) {
        final Map<String, Object> newMap = new HashMap<>();
        newMap.put(TS_LOCAL, LocalDateTime.now());
        jsonKeyMapping().keySet().forEach(k -> Optional.ofNullable(map.get(k)).ifPresent(v -> newMap.put(jsonKeyMapping().get(k), v)));
        return newMap;
    }
}
