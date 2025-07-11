package com.redis.demo.day1;

import jakarta.annotation.PostConstruct;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *  Redis를 활용하여 캐시 워밍업과 프리로딩 기법을 알아본다.
 *  - 시작 할 때 캐시 초기화
 *  - 예약된 캐시 새로고침
 *  - 선택적으로 캐시 워밍업
 *
 */
@Slf4j
@Service
public class CacheWarmingService {
    private final RedisTemplate<String, Object> redisTemplate;

    // 테스트 목적의 가상 데이터베이스를 구성
    private final Map<String, Product> productDatabase = new HashMap<>();
    private final Map<String, Category> categoryDatabase = new HashMap<>();

    public CacheWarmingService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;

        initializeDatabaseData();
    }
    
    private void initializeDatabaseData() {
        // 카테고리
        categoryDatabase.put("c1", new Category("c1", "전자제품"));
        categoryDatabase.put("c2", new Category("c2", "옷"));
        categoryDatabase.put("c3", new Category("c3", "책"));

        // 제품
        productDatabase.put("p1", new Product("p1", "스마트폰", 1240000.0, 50, "c1"));
        productDatabase.put("p2", new Product("p2", "노트북", 1240000.0, 30, "c1"));
        productDatabase.put("p3", new Product("p3", "헤드폰", 240000.0, 100, "c1"));
        productDatabase.put("p4", new Product("p4", "티셔츠", 40000.0, 300, "c2"));
        productDatabase.put("p5", new Product("p5", "청바지", 70000.0, 150, "c2"));
        productDatabase.put("p6", new Product("p6", "소설", 35000.0, 120, "c3"));
        productDatabase.put("p7", new Product("p7", "만화", 9000.0, 50, "c3"));
    }

    @PostConstruct
    public void warmCachedOnStartup() {
        log.info("시작 할 때 캐시 워밍업 중....");
        // 1. 모든 카테고리 미리 로드하기
        preloadAllCategories();

        // 2. 인기 제품 미리 로드하기
        preloadPopularProducts();

        log.info("캐시 워밍업 완료");
    }

    /**
     * 모든 카테고리를 캐시에 미리 로드하기
     *  - 카테고리는 데이터 자체가 작은 데이터셋이다.
     *  - 자주 접근될 가능성이 높다.
     *  그래서 캐시에 모두 로드하는 것이 합리적이다.
     */
    private void preloadAllCategories() {
        log.info("모든 카테고리를 미리 로드중입니다...");

        // 데이터베이스 접근 시뮬레이션 (지연관련) 딜레이
        simulateDatabaseDelay();

        categoryDatabase.forEach((id, category) -> {
            String cacheKey = "category:" + id;
            redisTemplate.opsForValue().set(cacheKey, category, 1, TimeUnit.DAYS);
            log.info("카테고리 미리 로드됨: {}", id);
        });
    }

    private void preloadPopularProducts() {
        log.info("인기 제품 미리 로드중입니다...");

        simulateDatabaseDelay();

        // 실제에서는 데이터베이스 조회를 하겠죠 쿼리를 날립니다.
        // 재고가 50보다 높은 제품을 인기 제품으로 간주하겠습니다.
        List<Product> popularProducts = productDatabase.values().stream()
                .filter(product -> product.getStock() > 50)
                .toList();

        // 인기 제품을 캐시에 로드하기
        popularProducts.forEach(product -> {
            String cacheKey = "product:" + product.getId();
            redisTemplate.opsForValue().set(cacheKey, product, 6, TimeUnit.HOURS);
            log.info("인기제품 미리 로드됨: {}", product.getId());
        });

        // 빠른 접근을 하기위해서 인기 제품의 아이디값 목록도 저장
        List<String> popularProductIds = popularProducts.stream()
                .map(Product::getId)
                .toList();

        redisTemplate.opsForValue().set("popular-products", popularProductIds, 6, TimeUnit.HOURS);
    }

    @Scheduled(fixedRate = 3600000)
    public void refreshCacheScheduled() {
        log.info("예약된 캐시 새로고침 시작...");
        // 1. 모든 카테고리 미리 로드하기
        preloadAllCategories();

        // 2. 인기 제품 미리 로드하기
        preloadPopularProducts();

        log.info("예약된 캐시 새로고침 완료");
    }

    public void warmCategoryProducts(String categoryId) {
        log.info("카테고리 {}의 제품에 대한 캐시 워밍업 중.", categoryId);

        simulateDatabaseDelay();

        List<Product> categoryProducts = productDatabase.values().stream()
                .filter(product -> product.getCategoryId().equals(categoryId))
                .toList();

        // 카테고리 제품을 캐시에 로드
        categoryProducts.forEach(product -> {
            String cacheKey = "product:" + product.getId();
            redisTemplate.opsForValue().set(cacheKey, product, 6, TimeUnit.HOURS);
            log.info("카테고리 {}에서 제품 {} 미리 로드됨.", categoryId, product.getId());
        });

        List<String> categoryProductIds = categoryProducts.stream()
                .map(Product::getId)
                .toList();

        redisTemplate.opsForValue().set("category:" + categoryId + ":products", categoryProductIds, 6, TimeUnit.HOURS);
    }

    public Product getProduct(String productId) {
        String cacheKey = "product:" + productId;

        // cache 퍼스트
        Product cachedProduct = (Product) redisTemplate.opsForValue().get(cacheKey);

        if (cachedProduct != null) {
            log.info("Cache hit for product: {}", productId);
            return cachedProduct;
        }

        log.info("캐시에 상품이 없음: {}, 데이터베이스에서 가져오기", productId);

        simulateDatabaseDelay();

        Product product = productDatabase.get(productId);

        if (product != null) {
            redisTemplate.opsForValue().set(cacheKey, product, 6, TimeUnit.HOURS);
            log.info("캐시에 상품 등록 완료: {}", productId);
        }

        return product;
    }

    public Category getCategory(String categoryId) {
        String cacheKey = "category:" + categoryId;

        // cache 퍼스트
        Category cachedCategory = (Category) redisTemplate.opsForValue().get(cacheKey);

        if (cachedCategory != null) {
            log.info("Cache hit for category: {}", categoryId);
            return cachedCategory;
        }

        log.info("캐시에 카테고리가 없음: {}, 데이터베이스에서 가져오기", categoryId);

        simulateDatabaseDelay();

        Category category = categoryDatabase.get(categoryId);

        if (category != null) {
            redisTemplate.opsForValue().set(cacheKey, category, 1, TimeUnit.DAYS);
            log.info("캐시에 카테고리 등록 완료: {}", categoryId);
        }

        return category;
    }

    private void simulateDatabaseDelay() {
        try {
            TimeUnit.MICROSECONDS.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
        private String categoryId;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class Category {
        private String id;
        private String name;
    }
}
