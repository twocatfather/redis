package com.redis.demo.day02;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class RealTimeRankingService {
    private final RedisTemplate<String, Object> redisTemplate;

    private static final String PRODUCT_VIEWS_KEY = "ranking:product:views";
    private static final String PRODUCT_VIEWS_HOURLY_KEY = "ranking:product:views:hourly";
    private static final String PRODUCT_VIEWS_DAILY_KEY = "ranking:product:views:daily";
    private static final String LEADERBOARD_KEY = "ranking:leaderboard";

    public void recordProductView(String productId) {

        redisTemplate.opsForZSet().incrementScore(PRODUCT_VIEWS_KEY, productId, 1);

        String hourlyKey = PRODUCT_VIEWS_HOURLY_KEY + getCurrentHour();
        redisTemplate.opsForZSet().incrementScore(hourlyKey, productId, 1);
        redisTemplate.expire(hourlyKey, 48, TimeUnit.HOURS);

        String dailyKey = PRODUCT_VIEWS_DAILY_KEY + getCurrentDate();
        redisTemplate.opsForZSet().incrementScore(dailyKey, productId, 1);
        redisTemplate.expire(dailyKey, 30, TimeUnit.DAYS);
    }

    private String getCurrentHour() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"));
    }

    private String getCurrentDate() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }
}
