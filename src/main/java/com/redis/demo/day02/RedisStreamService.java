package com.redis.demo.day02;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;

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

    public String publishOrderEvent(String orderId, String eventType, Map<String, String> data) {
        Map<String, String> event = new HashMap<>(data);
        event.put("orderId", orderId);
        event.put("eventType", eventType);
        event.put("timestamp", String.valueOf(System.currentTimeMillis()));

        RecordId recordId = redisTemplate.opsForStream().add(ORDER_EVENTS_STREAM, event);

        return recordId != null ? recordId.getValue() : null;
    }

    public List<Map<String, String>> getRecentOrderEvents(int count) {
        List<Map<String, String>> events = new ArrayList<>();

        for (int i = 0; i < Math.min(count, 5); i++) {
            Map<String, String> event = new HashMap<>();

            String orderId = "order-" + (1000 + i);

            event.put("orderId", orderId);
            event.put("eventId", "1234567890123-" + i);

            switch (i % 3) {
                case 0 -> {
                    event.put("eventType", "created");
                    event.put("productId", "product-" + (100 + i));
                    event.put("quantity", String.valueOf(1 + i));
                }

                case 1 -> {
                    event.put("eventType", "paid");
                    event.put("paymentMethod", "credit_card");
                    event.put("amount", String.valueOf(99.99 + i * 10));
                }

                case 2 -> {
                    event.put("eventType", "shipped");
                    event.put("carrier", "CJ");
                    event.put("trackingNumber", "CJ" + (9000000 + i));
                }
            }

            event.put("timestamp", String.valueOf(System.currentTimeMillis() - i * 60000));
            events.add(event);
        }

        return events;
    }
    
    public List<Map<String, String>> getRecentInventoryEvents(int count) {
        List<Map<String, String>> events = new ArrayList<>();
        
        for (int i = 0; i < Math.min(count, 5); i++) {
            Map<String, String> event = new HashMap<>();
            
            String productId = "product-" + (100 + i);
            
            event.put("productId", productId);
            event.put("eventId", "1234567890123-" + i);
            
            if (i % 2 == 0) {
                event.put("evetType", "stock_update");
                event.put("quantityChanged", String.valueOf(-1 - i));
                event.put("reason", "order_order-" + (1000 + i));
            } else {
                event.put("evetType", "low_stock");
                event.put("currentStock", String.valueOf(5 - i));
                event.put("threshold", "5");
            }

            event.put("timestamp", String.valueOf(System.currentTimeMillis() - i * 60000));
            events.add(event);
        }
        return events;
    }

    private class OrderEventStreamListener implements StreamListener<String, MapRecord<String, String, String>> {

        @Override
        public void onMessage(MapRecord<String, String, String> message) {
            Map<String, String> values = message.getValue();
            String orderId = values.get("orderId");
            String eventType = values.get("eventType");

            switch (eventType) {
                case "created" -> log.info("Processing order creation for order {}", orderId);
                case "paid" -> log.info("Processing payment for order: {}", orderId);
                case "shipped" -> log.info("Processing shipment for order: {}", orderId);
                default -> log.info("Unknown");
            }

            redisTemplate.opsForStream().acknowledge(ORDER_EVENTS_STREAM, ORDER_PROCESSING_GROUP, message.getId());
        }
    }

    private class InventoryEventStreamListener implements StreamListener<String, MapRecord<String, String, String>> {

        @Override
        public void onMessage(MapRecord<String, String, String> message) {
            // values -> map 을 먼저만든다.
            Map<String, String> values = message.getValue();
            String productId = values.get("productId");
            String eventType = values.get("eventType");

            if ("stock_update".equals(eventType)) {
                String quantityChanged = values.get("quantityChanged");
                String reason = values.get("reason");
                log.info("{}, {}", quantityChanged, reason);
            } else if ("low_stock".equals(eventType)) {
                log.info("Processing low stock alert for product: {}", productId);
            }
            // get -> productId, eventType
            // if else
            // values -> get
            // acknowledge -> INVENTORY_EVENTS_STREAM
            redisTemplate.opsForStream().acknowledge(INVENTORY_EVENTS_STREAM, INVENTORY_PROCESSING_GROUP, message.getId());
        }
    }
}
