package com.redis.demo.controller;

import com.redis.demo.day1.CacheWarmingService;
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
}
