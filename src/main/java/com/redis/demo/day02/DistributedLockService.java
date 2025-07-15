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

    public boolean decrementStock(String productId, int quantity) {
        String lockName = "inventory:lock:" + productId;
        RLock lock = redissonClient.getLock(lockName);

        try {
            boolean isLocked = lock.tryLock(5, 10, TimeUnit.SECONDS);

            if (isLocked) {
                try {
                    Product product = productInventory.get(productId);

                    if (product == null) {
                        return false;
                    }

                    if (product.getStock() < quantity) {
                        return false;
                    }

                    TimeUnit.MILLISECONDS.sleep(500);

                    product.setStock(product.getStock() - quantity);
                    return true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                } finally {
                    lock.unlock();
                }
            } else {
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public void performCriticalOperation(String operationId) {
        String lockName = "operation:lock:" + operationId;
        RLock lock = redissonClient.getLock(lockName);

        try {
            boolean isLocked = lock.tryLock(5, 30, TimeUnit.SECONDS);

            if (isLocked) {
                try {
                    log.info("Operation Lock 획득: {}", operationId);

                    log.info("Starting Critical Operation: {}", operationId);
                    TimeUnit.SECONDS.sleep(10);
                    log.info("Completed Critical Operation: {}", operationId);
                } catch (InterruptedException e) {
                    log.info("Critical Operation interrupted: {}", operationId);
                    Thread.currentThread().interrupt();
                } finally {
                    lock.unlock();
                    log.info("Operation lock 해제: {}", operationId);
                }
            } else {
                log.info("Critical Operation already running: {}", operationId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void demonstrateFairLock(String resourceId) {
        String lockName = "fair:lock:" + resourceId;
        RLock lock = redissonClient.getFairLock(lockName);

        try {
            boolean isLocked = lock.tryLock(5, 10, TimeUnit.SECONDS);

            if (isLocked) {
                try {
                    log.info("fair Lock 획득: {}", resourceId);


                    TimeUnit.SECONDS.sleep(2);
                    log.info("fair Lock 얻은 후 작업 완료: {}", resourceId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    lock.unlock();
                    log.info("fair lock 해제: {}", resourceId);
                }
            } else {
                log.info("락을 얻지 못했습니다: {}", resourceId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public int getProductStock(String productId) {
        Product product = productInventory.get(productId);
        return product != null ? product.getStock() : -1;
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
