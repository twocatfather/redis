package com.redis.demo.day1;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *  - Cache-Aside
 *  - Write-Through
 *  - Write-Behind
 */
@Slf4j
@Service
public class CachingStrategyService {
    private final RedisTemplate<String, Object> redisTemplate;
    private final Map<String, Product> productDatabase = new HashMap<>();

    public CachingStrategyService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;

        productDatabase.put("p1", new Product("p1", "Smartphone", 599.99, 50));
        productDatabase.put("p2", new Product("p2", "Laptop", 1299.99, 30));
        productDatabase.put("p3", new Product("p3", "Headphones", 199.99, 100));
    }

    /**
     *  지연 로딩 패턴
     *  1. 먼저 캐시를 확인
     *  2. 데이터가 캐시에 있으면 반환
     *  3. 없으면 데이터베이스에 쿼리를 날린다.
     *  4. 데이터베이스 결과를 캐시에 저장한다.
     *  5. 결과를 반환
     *
     *  -> 가장 일반적인 캐싱 패턴, 읽기위주의 워크로드에 적합
     */
    public Product getProductCacheAside(String productId) {
        String cacheKey = "product:" + productId;

        // 1. 먼저 캐시를 확인한다.
        Product cachedProduct = (Product) redisTemplate.opsForValue().get(cacheKey);

        if (cachedProduct != null) {
            // 2. 데이터가 캐시에 있으면 반환
            log.info("제품 {}에 대한 캐시 히트", productId);
            return cachedProduct;
        }

        // 3. 없으면 데이터베이스에 쿼리를 날린다.
        try {
            TimeUnit.MILLISECONDS.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 4. 데이터베이스 결과를 캐시에 저장한다.
        Product product = productDatabase.get(productId);
        if (product != null) {
            redisTemplate.opsForValue().set(cacheKey, product, 1, TimeUnit.HOURS);
            log.info("제품 {}을(를) 캐시에 저장함.", productId);
        }

        return product;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class Product {
        private String id;
        private String name;
        private double price;
        private int stock;
    }
}
