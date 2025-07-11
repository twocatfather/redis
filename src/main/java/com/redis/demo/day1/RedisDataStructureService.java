package com.redis.demo.day1;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisDataStructureService {
    private final RedisTemplate<String, Object> redisTemplate;

    public void demonstrateStringOperation() {
        log.info("Redis String 연산 시작...");

        // 문자열 값을 설정
        redisTemplate.opsForValue().set("product:1:name", "스마트폰");

        // 만료시간 함께설정
        redisTemplate.opsForValue().set("product:1:view-count", 100, 1, TimeUnit.HOURS);

        // 문자열 값 가져오기
        String productName = (String) redisTemplate.opsForValue().get("product:1:name");
        log.info("프로덕트 이름: {}", productName);

        // 카운트를 증가
        redisTemplate.opsForValue().increment("product:1:view-count");

        String viewCount = (String) redisTemplate.opsForValue().get("product:1:view-count");
        log.info("증가 후 조회수: {}", viewCount);
    }

    // hash  연산
    public void demonstrateHashOperation() {
        log.info("Redis Hash 연산 시작...");

        String key = "product:1";

        // hash에 여러 필드 에 저장
        redisTemplate.opsForHash().put(key, "name", "스마트폰");
        redisTemplate.opsForHash().put(key, "price", "50.00");
        redisTemplate.opsForHash().put(key, "category", "전자제품");

        // 여러필드를 한번에 저장
        Map<String, Object> productProfile = Map.of(
                "brand", "apple",
                "stock", 100,
                "rating", 4.7
        );
        redisTemplate.opsForHash().putAll(key, productProfile);

        String productName = (String) redisTemplate.opsForHash().get(key, "name");
        log.info("해시에서 가져온 제품 이름: {}", productName);

        Map<Object, Object> allFields = redisTemplate.opsForHash().entries(key);
        log.info("모든 필드: {}", allFields);

        redisTemplate.opsForHash().increment(key, "stock", -1);

        Object updatedStock = redisTemplate.opsForHash().get(key, "stock");
        log.info("업데이트된 재고량: {}", updatedStock);
    }

    public void demonstrateListOperations() {
        log.info("Redis List 연산 시작...");

        String key = "user:1:recent-views";

        // 리스트의 오른쪽에 항목을 추가
        redisTemplate.opsForList().rightPush(key, "product:1");
        redisTemplate.opsForList().rightPush(key, "product:2");
        redisTemplate.opsForList().rightPush(key, "product:3");

        // 여러항목을 한번에 넣기
        redisTemplate.opsForList().rightPushAll(key, "product:4", "product:5", "product:6");

        // 리스트 왼쪽(시작 부분) 에서 항목 추가
        redisTemplate.opsForList().leftPush(key, "product:0");

        // 리스트 크기 가져오기
        Long size = redisTemplate.opsForList().size(key);
        log.info("리스트 크기: {}", size);

        // 항목 범위 가져오기
        List<Object> allItems = redisTemplate.opsForList().range(key, 0, -1);
        log.info("리스트의 모든 항목: {}", allItems);

        Object lastViewed = redisTemplate.opsForList().rightPop(key);
        log.info("마지막으로 본 제품: {}", lastViewed);

        redisTemplate.opsForList().trim(key, 0, 2);

        List<Object> trimmedList = redisTemplate.opsForList().range(key, 0, -1);
        log.info("잘린 리스트: {}", trimmedList);
    }

    public void demonstrateSetOperations() {
        log.info("Redis Set 연산 시작...");

        redisTemplate.opsForSet().add("user:1:tags", "전자제품", "스마트폰", "it");
        redisTemplate.opsForSet().add("user:2:tags", "전자제품", "맥북", "아이폰", "애플");

        Set<Object> user1Tags = redisTemplate.opsForSet().members("user:1:tags");
        log.info("사용자 1의 태그: {}", user1Tags);

        // set에 값이 포함이 되어있는지 확인하기
        Boolean hasTag = redisTemplate.opsForSet().isMember("user:1:tags", "스마트폰");
        log.info("사용자 1이 '스마트폰' 태그를 가지고 있는가? : {}", hasTag);

        // 두 집합의 교집합을 가져오기  (공통태그)
        Set<Object> commonTags = redisTemplate.opsForSet().intersect("user:1:tags", "user:2:tags");

        // 합 집합
        Set<Object> allTags = redisTemplate.opsForSet().union("user:1:tags", "user:2:tags");

        // 차 집합
        Set<Object> uniqueTags = redisTemplate.opsForSet().difference("user:1:tags", "user:2:tags");

        redisTemplate.opsForSet().remove("user:1:tags", "it");

        Object randomTag = redisTemplate.opsForSet().randomMember("user:1:tags");
        log.info("사용자 1의 무작위 태그: {}", randomTag);
    }

    public void demonstrateSortedSetOperations() {
        log.info("Redis Sorted Set 연산 시작...");

        String key = "product:trending";

        // 점수와 함께 항목을 추가
        redisTemplate.opsForZSet().add(key, "product:1", 100);
        redisTemplate.opsForZSet().add(key, "product:2", 150);
        redisTemplate.opsForZSet().add(key, "product:3", 75);
        redisTemplate.opsForZSet().add(key, "product:4", 70);
        redisTemplate.opsForZSet().add(key, "product:5", 200);

        // 항목의 점수를 가져오기
        Double score = redisTemplate.opsForZSet().score(key, "product:2");
        log.info("product:2의 점수: {}", score);

        // 점수를 증가
        redisTemplate.opsForZSet().incrementScore(key, "product:3", 40);

        // 업데이트된 점수를 가져오기
        Double updatedScore = redisTemplate.opsForZSet().score(key, "product:3");
        log.info("product:3의 점수: {}", updatedScore);

        // 순위 가져오기
        Long rank = redisTemplate.opsForZSet().rank(key, "product:1");
        log.info("product:1의 순위 (오름차순): {}", rank);

        Long reverseRank = redisTemplate.opsForZSet().reverseRank(key, "product:1");
        log.info("product:1의 순위 (내림차순): {}", reverseRank);

        Set<Object> topProducts = redisTemplate.opsForZSet().reverseRange(key, 0, 2);
        log.info("상위 3개 인기 제품: {}", topProducts);

        Set<Object> productsRange = redisTemplate.opsForZSet().rangeByScore(key, 100, 200);
        log.info("점수가 100 에서 200 사이인 제품: {}", productsRange);

        Long count = redisTemplate.opsForZSet().zCard(key);
        log.info("인기 제품의 총 개수: {}", count);
    }
}
