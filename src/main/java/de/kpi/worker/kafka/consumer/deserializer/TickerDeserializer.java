package de.kpi.worker.kafka.consumer.deserializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class TickerDeserializer implements Deserializer<List<KafkaMessage>> {

    final ObjectMapper mapper = new ObjectMapper();

    @Override
    public List<KafkaMessage> deserialize(final String topic, final byte[] data) {
        List<KafkaMessage> list = new ArrayList<>();
        try {
            final Map<String, Object> map = mapper.readValue(data, new TypeReference<>() {
            });
            Optional.ofNullable(map.get("data")).filter(v -> v instanceof List).ifPresent(m -> ((List<Map<String, Object>>) m).forEach(a -> {
                final KafkaMessage filterAsk = filterAsk(a, Arrays.asList("askPrice"));
                final KafkaMessage filterBid = filterBid(a, Arrays.asList("bidPrice"));
                final KafkaMessage filterTime = filterTime(a, Arrays.asList("timestamp"));
                final KafkaMessage filterSymbol = filterSymbol(a, Arrays.asList("symbol"));
                list.add(buildKafkaMessageDto(filterAsk.getAsk(), filterBid.getBid(), filterTime.getTsTicker(), filterSymbol.getSymbol(), topic));
            }));
            Optional.ofNullable(map.get("data")).filter(v -> v instanceof Map).ifPresent(m -> {
                final Map<String, Object> mp = (Map<String, Object>) m;
                final KafkaMessage filterAsk = filterAsk(mp, Arrays.asList("a"));
                final KafkaMessage filterBid = filterBid(mp, Arrays.asList("b"));
                final KafkaMessage filterTime = filterTime(mp, Arrays.asList("T"));
                final KafkaMessage filterSymbol = filterSymbol(mp, Arrays.asList("s"));
                list.add(buildKafkaMessageDto(filterAsk.getAsk(), filterBid.getBid(), filterTime.getTsTicker(), filterSymbol.getSymbol(), topic));
            });
            Optional.of(map).filter(k -> !k.containsKey("data")).ifPresent(m -> {
                final KafkaMessage filterAsk = filterAsk(m, Arrays.asList("ask"));
                final KafkaMessage filterBid = filterBid(m, Arrays.asList("bid"));
                final KafkaMessage filterTime = filterTime(m, Arrays.asList("time"));
                final KafkaMessage filterSymbol = filterSymbol(m, Arrays.asList("pair"));
                list.add(buildKafkaMessageDto(filterAsk.getAsk(), filterBid.getBid(), filterTime.getTsTicker(), filterSymbol.getSymbol(), topic));
            });
        } catch (Exception ex) {
            log.error("Can't deserialize: ", ex);
        }
        return list;
    }

    private KafkaMessage filterAsk(final Map<String, Object> map, final List<String> keys) {
        final KafkaMessage kafkaMessageDto = new KafkaMessage();
        keys.forEach(key -> Optional.ofNullable(map.get(key)).ifPresent(value -> kafkaMessageDto.setAsk(Double.parseDouble(value.toString()))));
        return kafkaMessageDto;
    }

    private KafkaMessage filterBid(final Map<String, Object> map, final List<String> keys) {
        final KafkaMessage kafkaMessageDto = new KafkaMessage();
        keys.forEach(key -> Optional.ofNullable(map.get(key)).ifPresent(value -> kafkaMessageDto.setBid(Double.parseDouble(value.toString()))));
        return kafkaMessageDto;
    }

    private KafkaMessage filterTime(final Map<String, Object> map, final List<String> keys) {
        final KafkaMessage kafkaMessageDto = new KafkaMessage();
        keys.forEach(key -> Optional.ofNullable(map.get(key)).ifPresent(value -> kafkaMessageDto.setTsTicker(value.toString())));
        return kafkaMessageDto;
    }

    private KafkaMessage filterSymbol(final Map<String, Object> map, final List<String> keys) {
        final KafkaMessage kafkaMessageDto = new KafkaMessage();
        keys.forEach(key -> Optional.ofNullable(map.get(key)).ifPresent(value -> kafkaMessageDto.setSymbol(value.toString())));
        return kafkaMessageDto;
    }

    private KafkaMessage buildKafkaMessageDto(final Double ask, final Double bid, final String time, final String symbol, final String exchange) {
        KafkaMessage dto = new KafkaMessage();
        dto.setAsk(ask);
        dto.setBid(bid);
        dto.setTsTicker(time);
        dto.setSymbol(symbol);
        dto.setTsLocal(new Date().toString());
        dto.setMid(Double.sum(dto.getAsk(), dto.getBid()) / 2);
        dto.setExchange(exchange);
        return dto;
    }

}
