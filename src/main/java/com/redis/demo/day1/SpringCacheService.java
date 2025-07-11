package com.redis.demo.day1;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class SpringCacheService {

    private final Map<Long, Product> productDatabase = new HashMap<>();

    public SpringCacheService() {
        productDatabase.put(1L, new Product(1L, "Smartphone", 599.99, 50));
        productDatabase.put(2L, new Product(2L, "Laptop", 1299.99, 30));
        productDatabase.put(3L, new Product(3L, "Headphones", 199.99, 100));
        productDatabase.put(4L, new Product(4L, "Tablet", 499.99, 25));
        productDatabase.put(5L, new Product(5L, "Smartwatch", 299.99, 40));
    }

    @Cacheable(value = "products", key = "#productId")
    public Product getProduct(Long productId) {
        log.info("데이터베이스에서 ID가 {}인 제품을 가져오는 중", productId);

        try {
            TimeUnit.MILLISECONDS.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return productDatabase.get(productId);
    }

    @CachePut(value = "products", key = "#product.id")
    public Product updateProduct(Product product) {
        log.info("ID가 {}인 제품을 업데이트 중", product.getId());

        productDatabase.put(product.getId(), product);
        return product;
    }

    @CacheEvict(value = "products", key = "#productId")
    public void deleteProduct(Long productId) {
        log.info("ID가 {}인 제품을 삭제중", productId);

        productDatabase.remove(productId);
    }

    @CacheEvict(value = "products", allEntries = true)
    public void clearProductCache() { log.info("제품 캐시를 모두 비우는중"); }

    @Cacheable(value = "available-products", key = "#productId", condition = "#result != null && #result.stock > 0")
    public Product getAvailableProduct(Long productId) {
        log.info("ID가 {}인 이용 가능한 제품을 가져오는 중", productId);

        try {
            TimeUnit.MILLISECONDS.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return productDatabase.get(productId);
    }

    @Cacheable(value = "product-details", key = "#productId", unless = "#result != null && #result.stock <= 0")
    public Product getProductDetails(Long productId) {
        log.info("ID가 {}인 제품 상세를 가져오는 중", productId);

        try {
            TimeUnit.MILLISECONDS.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return productDatabase.get(productId);
    }



    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class Product {
        private Long id;
        private String name;
        private double price;
        private int stock;
    }
}
