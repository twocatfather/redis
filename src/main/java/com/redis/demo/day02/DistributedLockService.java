package com.redis.demo.day02;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class DistributedLockService {
    private final RedissonClient redissonClient;

    private final Map<String, Product> productInventory = new HashMap<>();

    public DistributedLockService(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
        productInventory.put("p1", new Product("p1", "Smartphone", 50));
        productInventory.put("p2", new Product("p2", "Laptop", 30));
        productInventory.put("p3", new Product("p3", "Headphones", 100));
    }

    public void demonstrateBasicLock(String resourceId) {
        String lockName = "lock:" + resourceId;
        RLock lock = redissonClient.getLock(lockName);

        try {
            boolean isLocked = lock.tryLock(5, 10, TimeUnit.SECONDS);

            if (isLocked) {
                log.info("Lock 을 얻었습니다. {}", resourceId);

                try {
                    TimeUnit.SECONDS.sleep(2);
                    log.info("접근완료되어 데이터를 가져왔습니다.: {}", resourceId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    lock.unlock();
                    log.info("락이 해제 되었습니다.");
                }
            } else {
                log.info("락을 얻지 못했습니다.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class Product {
        private String id;
        private String name;
        private int stock;
    }
}
