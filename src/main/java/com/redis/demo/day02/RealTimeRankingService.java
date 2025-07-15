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

    /**
     * 리더보드에서 특정 사용자 주변의 사용자들을 가져옵니다.
     *
     * @param userId 사용자의 ID
     * @param range 위아래로 포함할 사용자 수
     * @return 사용자 ID와 점수 목록
     */
    public List<RankedItem> getUsersAroundInLeaderboard(String userId, int range) {
        Long userRank = redisTemplate.opsForZSet().reverseRank(LEADERBOARD_KEY, userId);

        if (userRank == null) {
            log.warn("User not found in leaderboard: {}", userId);
            return Collections.emptyList();
        }

        // Calculate the range of ranks to retrieve
        long startRank = Math.max(0, userRank - range);
        long endRank = userRank + range;

        Set<ZSetOperations.TypedTuple<Object>> usersAround =
                redisTemplate.opsForZSet().reverseRangeWithScores(LEADERBOARD_KEY, startRank, endRank);

        return convertToRankedItems(usersAround);
    }

    /**
     * 특정 날짜의 가장 인기 있는 제품을 가져옵니다.
     *
     * @param date YYYY-MM-DD 형식의 날짜
     * @param limit 반환할 최대 제품 수
     * @return 제품 ID와 조회수 목록
     */
    public List<RankedItem> getMostPopularProductsByDay(String date, int limit) {
        String dailyKey = PRODUCT_VIEWS_DAILY_KEY + date;

        if (!Boolean.TRUE.equals(redisTemplate.hasKey(dailyKey))) {
            log.warn("No data available for date: {}", date);
            return Collections.emptyList();
        }

        Set<ZSetOperations.TypedTuple<Object>> popularProducts =
                redisTemplate.opsForZSet().reverseRangeWithScores(dailyKey, 0, limit - 1);

        return convertToRankedItems(popularProducts);
    }

    @RequiredArgsConstructor
    @Getter
    @ToString
    public static class RankedItem {
        private final String id;
        private final double score;
    }
}
