package com.redis.demo.controller;

import com.redis.demo.day02.DistributedLockService;
import com.redis.demo.day1.CacheWarmingService;
import com.redis.demo.day1.RedisDataStructureService;
import com.redis.demo.day1.SpringCacheService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/redis-demo")
@RequiredArgsConstructor
public class RedisDemoController {
    private final CacheWarmingService cacheWarmingService;
    private final RedisDataStructureService redisDataStructureService;
    private final SpringCacheService springCacheService;
    private final DistributedLockService distributedLockService;

    @GetMapping("/day1/data-structures")
    public ResponseEntity<Map<String, String>> demonstrateDataStructures() {
        redisDataStructureService.demonstrateStringOperation();
        redisDataStructureService.demonstrateHashOperation();
        redisDataStructureService.demonstrateListOperations();
        redisDataStructureService.demonstrateSetOperations();
        redisDataStructureService.demonstrateSortedSetOperations();

        Map<String, String> response = new HashMap<>();
        response.put("message", "Redis data structures demonstration completed. Check server logs for details.");
        return ResponseEntity.ok(response);
    }

    @GetMapping("/day1/cache-warming")
    public ResponseEntity<Map<String, Object>> demonstrateCacheWarming() {
        CacheWarmingService.Product product = cacheWarmingService.getProduct("p1");
        CacheWarmingService.Category category = cacheWarmingService.getCategory("c1");

        cacheWarmingService.warmCategoryProducts("c2");

        Map<String, Object> response = new HashMap<>();
        response.put("message", "캐시 완료");
        response.put("product", product);
        response.put("category", category);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/day1/spring-cache")
    public ResponseEntity<Map<String, Object>> demonstrateSpringCache() {
        // First call - will hit the database
        SpringCacheService.Product product1 = springCacheService.getProduct(1L);

        // Second call - will hit the cache
        SpringCacheService.Product product1Again = springCacheService.getProduct(1L);

        // Update the product - will update the cache
        product1.setStock(product1.getStock() + 10);
        SpringCacheService.Product updatedProduct = springCacheService.updateProduct(product1);

        // Get available products
        SpringCacheService.Product availableProduct = springCacheService.getAvailableProduct(2L);

        Map<String, Object> response = new HashMap<>();
        response.put("message", "Spring Cache demonstration completed.");
        response.put("product", product1);
        response.put("updatedProduct", updatedProduct);
        response.put("availableProduct", availableProduct);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/day2/distributed-locks")
    public ResponseEntity<Map<String, Object>> demonstrateDistributedLocks() {
        distributedLockService.demonstrateBasicLock("resource1");

        String productId = "p1";
        int initialStock = distributedLockService.getProductStock(productId);
        boolean decrementResult = distributedLockService.decrementStock(productId, 2);
        int updatedStock = distributedLockService.getProductStock(productId);

        // critical
        distributedLockService.performCriticalOperation("daily-report");

        distributedLockService.demonstrateFairLock("resource2");

        Map<String, Object> response = new HashMap<>();
        response.put("message", "분산락 데모 완료");
        response.put("productId", productId);
        response.put("initialStock", initialStock);
        response.put("decrementResult", decrementResult);
        response.put("updatedStock", updatedStock);
        return ResponseEntity.ok(response);
    }
}
