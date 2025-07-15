package com.redis.demo.day02;


import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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

    public List<RankedItem> getTopViewedProducts(int limit) {
        Set<ZSetOperations.TypedTuple<Object>> topProducts =
                redisTemplate.opsForZSet().reverseRangeWithScores(PRODUCT_VIEWS_KEY, 0, limit - 1);

        return convertToRankedItems(topProducts);
    }

    private String getCurrentHour() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"));
    }

    private String getCurrentDate() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

    private List<RankedItem> convertToRankedItems(Set<ZSetOperations.TypedTuple<Object>> tuples) {
        if (tuples == null) {
            return Collections.emptyList();
        }

        return tuples.stream()
                .map(tuple -> new RankedItem(
                        tuple.getValue() != null ? tuple.getValue().toString() : "",
                        tuple.getScore() != null ? tuple.getScore() : 0.0)
                )
                .toList();
    }

    public List<RankedItem> getTrendingProducts(int limit) {
        String tempKey = "ranking:trending:temp:" + UUID.randomUUID().toString();

        try {
            double[] weights = {1.0, 0.8, 0.6, 0.4, 0.2, 0.1};

            for (int i = 0; i < weights.length; i++) {
                String hourKey = PRODUCT_VIEWS_HOURLY_KEY + getHourOffset(i);

                if (!Boolean.TRUE.equals(redisTemplate.hasKey(hourKey))) {
                    continue;
                }

                redisTemplate.opsForZSet().unionAndStore(
                        tempKey,
                        Collections.singleton(hourKey),
                        tempKey
                );

                Set<Object> members = redisTemplate.opsForZSet().range(tempKey, 0, -1);
                if (members != null) {
                    for (Object member : members) {
                        Double score = redisTemplate.opsForZSet().score(tempKey, member);
                        if (score != null) {
                            redisTemplate.opsForZSet().add(tempKey, member, score * weights[i]);
                        }
                    }
                }
            }

            Set<ZSetOperations.TypedTuple<Object>> trendingProducts =
                    redisTemplate.opsForZSet().reverseRangeWithScores(tempKey, 0, limit - 1);

            return convertToRankedItems(trendingProducts);
        } finally {
            redisTemplate.delete(tempKey);
        }
    }

    private String getHourOffset(int hourAgo) {
        return LocalDateTime.now()
                .minusHours(hourAgo)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"));
    }

    public void updateLeaderboardScore(String userId, double score) {
        redisTemplate.opsForZSet().incrementScore(LEADERBOARD_KEY, userId, score);
    }

    public List<RankedItem> getTopLeaderboardUsers(int limit) {
        Set<ZSetOperations.TypedTuple<Object>> topUsers =
                redisTemplate.opsForZSet().reverseRangeWithScores(LEADERBOARD_KEY, 0, limit - 1);
        return convertToRankedItems(topUsers);
    }

    public long getUserLeaderboardRank(String userId) {
        Long reverseRank = redisTemplate.opsForZSet().reverseRank(LEADERBOARD_KEY, userId);
        return reverseRank != null ? reverseRank : -1;
    }

    @RequiredArgsConstructor
    @Getter
    @ToString
    public static class RankedItem {
        private final String id;
        private final double score;
    }
}
