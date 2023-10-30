package com.hmdp.utils;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_TTL;

/**
 * @author zhangxiulin
 * @date 2023/10/24 20:22
 * @description redis 工具类
 */
@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate redisTemplate;

    @Autowired
    public CacheClient(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 将任意对象序列化为 json 存入 redis
     *
     * @param key   键
     * @param value 值
     * @param time  时间
     * @param unit  时间单位
     */
    public void set(String key, Object value, Long time, TimeUnit unit) {
        redisTemplate.opsForValue().set(key, JSON.toJSONString(value), time, unit);
    }

    /**
     * 将任意对象序列化为 json 存入 redis，携带逻辑过期时间，用于解决 redis 缓存中的缓存击穿的问题。
     *
     * @param key   键
     * @param value 值
     * @param time  时间
     * @param unit  时间单位
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //存入 redis，设置为永久有效
        redisTemplate.opsForValue().set(key, JSON.toJSONString(redisData));
    }

    /**
     * 利用缓存空置解决缓存穿透的问题
     */
    public <R, ID> R queryWithPassThrough(String keyPrefix,
                                          ID id,
                                          Class<R> type,
                                          Function<ID, R> dbFallback,
                                          Long time,
                                          TimeUnit unit) {
        String key = keyPrefix + id;
        String json = redisTemplate.opsForValue().get(key);
        if (StringUtils.isNotBlank(json)) {
            //Redis 中存在值，直接返回即可
            return JSON.parseObject(json, type);
        } else if (StringUtils.equals("", json)) {
            //缓存的是空置，直接返回 null
            return null;
        }

        //数据库不存在，直接查询数据库
        R r = dbFallback.apply(id);
        if (Objects.isNull(r)) {
            //数据库不存在，缓存空值
            this.set(key, "", time, unit);
            return null;
        } else {
            //数据库存在，缓存到 Redis
            this.set(key, r, time, unit);
            return r;
        }
    }

    /**
     * 利用逻辑过期时间解决缓存击穿的问题
     */
    public <R, ID> R queryWithLogicalExpire(String keyPrefix,
                                            ID id,
                                            Class<R> type,
                                            Function<ID, R> dbFallback,
                                            Long time,
                                            TimeUnit unit) {
        String key = keyPrefix + id;
        String json = redisTemplate.opsForValue().get(key);
        if (StringUtils.isBlank(json)) {
            //这里直接返回 null，是因为之前做过缓存预热了，没有查到数据直接返回空即可
            return null;
        }

        RedisData redisData = JSON.parseObject(json, RedisData.class);
        JSONObject jsonObject = (JSONObject) redisData.getData();
        //返回出去给的那个类型
        R r = BeanUtil.toBean(jsonObject, type);

        LocalDateTime expireTime = redisData.getExpireTime();
        if (expireTime != null && expireTime.isAfter(LocalDateTime.now())) {
            //未过期，直接返回
            return r;
        }

        //处理已过期的场景
        //获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean lock = tryLock(lockKey);
        if (lock) {
            //异步更新数据
            CompletableFuture.runAsync(() -> {
                try {
                    //查询数据库
                    R r2 = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, r2, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }

        //没有成功获取到锁，返回过期的商铺信息
        return r;
    }

    /**
     * 加锁
     */
    private boolean tryLock(String key) {
        Boolean flag = redisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 解锁
     */
    private void unlock(String key) {
        redisTemplate.delete(key);
    }
}
