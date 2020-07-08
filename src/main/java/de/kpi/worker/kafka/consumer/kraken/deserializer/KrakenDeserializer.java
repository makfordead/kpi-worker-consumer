package de.kpi.worker.kafka.consumer.kraken.deserializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.EXCHANGE;
import static de.kpi.worker.kafka.consumer.constant.JsonKeyConstant.MID;
import static de.kpi.worker.kafka.consumer.util.JsonKeyUtils.calculateMid;
import static de.kpi.worker.kafka.consumer.util.JsonKeyUtils.filterMap;

@Slf4j
public class KrakenDeserializer implements Deserializer<List<Map<String, Object>>> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public List<Map<String, Object>> deserialize(final String topic, final byte[] data) {
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            final Map<String, Object> map = mapper.readValue(data, new TypeReference<>() {
            });
            Optional.of(map).ifPresent(m -> {
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
}
