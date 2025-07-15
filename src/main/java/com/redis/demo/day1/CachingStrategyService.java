package com.redis.demo.day1;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 *  - Cache-Aside
 *  - Write Through
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

    public void updateProductCacheAside(Product product) {
        String productId = product.getId();
        String cacheKey = "product:" + productId;

        log.info("데이터베이스에서 제품 {} 업데이트중", productId);
        productDatabase.put(productId, product);

        redisTemplate.delete(cacheKey);

        // redisTemplate.opsForValue().set(cacheKey, product, 1, TimeUnit.Hours);
    }

    /**
     * Write-Through 패턴
     * 1. 데이터베이스를 업데이트
     * 2. 같은 트랜잭션에서 캐시 업데이트
     */

    public void updateProductWriteThrough(Product product) {
        String productId = product.getId();
        String cacheKey = "product:wt:" + productId;

        log.info("데이터베이스에서 제품 {} 업데이트중", productId);
        productDatabase.put(productId, product);

//        redisTemplate.delete(cacheKey);

        redisTemplate.opsForValue().set(cacheKey, product, 1, TimeUnit.HOURS);
    }

    public Product getProductWriteThrough(String productId) {
        String cacheKey = "product:wt:" + productId;

        Product cachedProduct = (Product) redisTemplate.opsForValue().get(cacheKey);

        if (cachedProduct != null) {
            return cachedProduct;
        }

        try {
            TimeUnit.MILLISECONDS.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Product product = productDatabase.get(productId);
        if (product != null) {
            redisTemplate.opsForValue().set(cacheKey, product, 1, TimeUnit.HOURS);
        }

        return product;
    }

    /**
     *  Write-Behind
     *  1. 캐시를 즉시 업데이트
     *  2. 비동기적으로 데이터베이스에 접근하여 저장
     */
    public void updateProductWriteBehind(Product product) {
        String productId = product.getId();
        String cacheKey = "product:wb:" + productId;

        redisTemplate.opsForValue().set(cacheKey, product, 1, TimeUnit.HOURS);

        CompletableFuture.runAsync(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            log.info("Write-Behind: 데이터베이스에서 제품 {}의 비동기 업데이트 중", productId);
            productDatabase.put(productId, product);
        });
    }

    public Product getProductWriteBehind(String productId) {
        String cacheKey = "product:wb:" + productId;

        Product cachedProduct = (Product) redisTemplate.opsForValue().get(cacheKey);

        if (cachedProduct != null) {
            return cachedProduct;
        }

        try {
            TimeUnit.MILLISECONDS.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Product product = productDatabase.get(productId);
        if (product != null) {
            redisTemplate.opsForValue().set(cacheKey, product, 1, TimeUnit.HOURS);
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
