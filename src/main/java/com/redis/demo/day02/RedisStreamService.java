package com.redis.demo.day02;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisStreamService {
    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisConnectionFactory connectionFactory;

    private static final String ORDER_EVENTS_STREAM = "order-events";
    private static final String INVENTORY_EVENTS_STREAM = "inventory-events";

    private static final String ORDER_PROCESSING_GROUP = "order-processors";
    private static final String INVENTORY_PROCESSING_GROUP = "inventory-processors";

    private StreamMessageListenerContainer<String, MapRecord<String, String, String>> listenerContainer;
    private List<Subscription> subscriptions = new ArrayList<>();

    private void createStreamsIfNotExists() {
        StreamOperations<String, Object, Object> streamOps = redisTemplate.opsForStream();

        if (!Boolean.TRUE.equals(redisTemplate.hasKey(ORDER_EVENTS_STREAM))) {
            Map<String, String> initialEvent = Collections.singletonMap("info", "Stream created");
            streamOps.add(ORDER_EVENTS_STREAM, initialEvent);
        }

        if (!Boolean.TRUE.equals(redisTemplate.hasKey(INVENTORY_EVENTS_STREAM))) {
            Map<String, String> initialEvent = Collections.singletonMap("info", "Stream created");
            streamOps.add(INVENTORY_EVENTS_STREAM, initialEvent);
        }
    }

    private void createConsumerGroupsIfNotExists() {
        try {
            redisTemplate.opsForStream().createGroup(ORDER_EVENTS_STREAM, ReadOffset.from("0"), ORDER_PROCESSING_GROUP);
        } catch (Exception e) {
            log.error("이미 소모한 주문 프로세스 혹은 생성할 수 없습니다.: {}", e.getMessage());
        }

        try {
            redisTemplate.opsForStream().createGroup(INVENTORY_EVENTS_STREAM, ReadOffset.from("0"), INVENTORY_PROCESSING_GROUP);
        } catch (Exception e) {
            log.error("이미 소모한 저장 프로세스 혹은 생성할 수 없습니다.: {}", e.getMessage());
        }
    }

    private void startStreamListeners() {
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                .builder()
                .pollTimeout(Duration.ofSeconds(1))
                .build();

        listenerContainer = StreamMessageListenerContainer.create(connectionFactory, options);

        Consumer orderConsumer = Consumer.from(ORDER_PROCESSING_GROUP, "consumer-1");
        StreamOffset<String> orderStreamOffset = StreamOffset.create(ORDER_EVENTS_STREAM, ReadOffset.lastConsumed());

        Subscription orderSubscription = listenerContainer.receive(
                orderConsumer,
                orderStreamOffset,
                new OrderEventStreamListener()
        );

        subscriptions.add(orderSubscription);

        Consumer inventoryConsumer = Consumer.from(INVENTORY_PROCESSING_GROUP, "consumer-1");
        StreamOffset<String> inventoryStreamOffset = StreamOffset.create(INVENTORY_EVENTS_STREAM, ReadOffset.lastConsumed());

        Subscription inventorySubscription = listenerContainer.receive(
                inventoryConsumer,
                inventoryStreamOffset,
                new InventoryEventStreamListener()
        );

        subscriptions.add(inventorySubscription);

        listenerContainer.start();
    }

    private class OrderEventStreamListener implements StreamListener<String, MapRecord<String, String, String>> {

        @Override
        public void onMessage(MapRecord<String, String, String> message) {

        }
    }

    private class InventoryEventStreamListener implements StreamListener<String, MapRecord<String, String, String>> {

        @Override
        public void onMessage(MapRecord<String, String, String> message) {

        }
    }
}
