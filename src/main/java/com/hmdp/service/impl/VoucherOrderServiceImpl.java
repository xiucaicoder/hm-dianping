package com.hmdp.service.impl;

import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.google.common.collect.Maps;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.RedissonDistributedLocker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Map;

import static com.hmdp.constant.CommonConstant.LOCK_WAIT_TIME;
import static com.hmdp.constant.CommonConstant.MAX_LEASE_TIME;
import static com.hmdp.constant.RedisKeyConstant.SEC_KILL_KEY;
import static com.hmdp.constant.RedisKeyConstant.VOUCHER_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl
        extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
        implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonDistributedLocker locker;
    @Resource
    private RabbitTemplate rabbitTemplate;

    @Override
    public Result seckillVoucher(Long voucherId) {
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        String lockKey = String.format(SEC_KILL_KEY, voucherId);

        try {
            boolean getLock = locker.tryLock(lockKey, LOCK_WAIT_TIME, MAX_LEASE_TIME);
            if (!getLock) {
                //获取锁失败
                return Result.fail("活动火爆，请刷新重试！");
            }

            //获取优惠券
            SeckillVoucher voucher = getVoucher(voucherId);

            if (!isOnePersonOneOrderBySet(voucherId)) {
                //不是一人一单
                return Result.fail("活动火爆，请刷新重试！");
            }

            //判断秒杀是否开始
            if (!isSeckillStarted(voucher)) {
                //尚未开始
                return Result.fail("秒杀尚未开始！");
            }

            //判断秒杀是否已经结束
            if (isSeckillEnded(voucher)) {
                //已经结束
                return Result.fail("秒杀已经结束！");
            }

            //判断库存是否充足
            if (!isStockEnough(voucher)) {
                //库存不足
                return Result.fail("库存不足！");
            }

            //判断 Redis 库存是否扣减成功
            if (!deductRedisStock(voucherId)) {
                return Result.fail("库存不足！");
            }

            long orderId = redisIdWorker.nextId("order");
            //发送秒杀消息
            this.sendSeckillMessage(userId, voucherId, orderId);

            return Result.ok(orderId);
        } finally {
            locker.unlock(lockKey);
        }
    }

    /**
     * 扣减 Redis 库存
     */
    private boolean deductRedisStock(Long voucherId) {
        String voucherKey = String.format(VOUCHER_KEY, voucherId);
        String voucherJson = stringRedisTemplate.opsForValue().get(voucherKey);

        if (StringUtils.isBlank(voucherJson)) {
            log.error("扣减库存失败，优惠券id：{}", voucherId);
            return false;
        }

        SeckillVoucher seckillVoucher = JSON.parseObject(voucherJson, SeckillVoucher.class);
        if (seckillVoucher.getStock() <= 0) {
            log.error("扣减库存失败，优惠券id：{}", voucherId);
            return false;
        }

        seckillVoucher.setStock(seckillVoucher.getStock() - 1);
        stringRedisTemplate.opsForValue().set(voucherKey, JSON.toJSONString(seckillVoucher));

        return true;
    }

    /**
     * 获取优惠券
     */
    private SeckillVoucher getVoucher(Long voucherId) {
        String voucherKey = String.format(VOUCHER_KEY, voucherId);
        String voucherJson = stringRedisTemplate.opsForValue().get(voucherKey);

        if (StringUtils.isNotBlank(voucherJson)) {
            return JSON.parseObject(voucherJson, SeckillVoucher.class);
        } else {
            SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
            if (voucher != null) {
                //30 分钟过期
                stringRedisTemplate.opsForValue().set(voucherKey, JSON.toJSONString(voucher), 30 * 60);
            }
            return voucher;
        }
    }

    /**
     * 发送秒杀消息
     */
    private void sendSeckillMessage(Long userId, Long voucherId, Long orderId) {
        if (userId == null || voucherId == null || orderId == null) {
            return;
        }

        Map<String, String> message = Maps.newHashMap();
        message.put("userId", String.valueOf(userId));
        message.put("voucherId", String.valueOf(voucherId));
        message.put("orderId", String.valueOf(orderId));

        log.warn("发送秒杀消息：{}", message);

        rabbitTemplate.convertAndSend("seckillQueue", message);
    }

    private boolean isSeckillStarted(SeckillVoucher voucher) {
        return voucher.getBeginTime().isBefore(LocalDateTime.now());
    }

    private boolean isSeckillEnded(SeckillVoucher voucher) {
        return voucher.getEndTime().isBefore(LocalDateTime.now());
    }

    private boolean isStockEnough(SeckillVoucher voucher) {
        return voucher.getStock() > 0;
    }

    /**
     * 通过 redis 的 Set 高效判断是否是一人一单
     */
    private boolean isOnePersonOneOrderBySet(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        String userVoucherKey = "user_voucher:" + voucherId;

        //获取该用户在Set中的状态
        Boolean isMember = stringRedisTemplate.opsForSet().isMember(userVoucherKey, userId.toString());
        if (isMember == null || !isMember) {
            //如果该用户没有购买过该商品，那么在Set中添加该用户的ID
            stringRedisTemplate.opsForSet().add(userVoucherKey, userId.toString());
            return true;
        } else {
            //如果该用户已经购买过该商品，那么返回false
            return false;
        }
    }

    /**
     * 判断是否是一人一单
     *
     * @return true:是 false:否
     */
    private boolean isOnePersonOneOrderByDb(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        String userVoucherLockKey = "user_voucher_lock:" + userId + ":" + voucherId;

        try {
            boolean getLock = locker.tryLock(userVoucherLockKey, LOCK_WAIT_TIME, MAX_LEASE_TIME);
            if (!getLock) {
                //获取锁失败
                return false;
            }

            Integer count = super.lambdaQuery()
                    .eq(VoucherOrder::getUserId, userId)
                    .eq(VoucherOrder::getVoucherId, voucherId)
                    .count();
            return count == 0;
        } finally {
            locker.unlock(userVoucherLockKey); // 解锁
        }
    }

    /**
     * 通过 redis 的 bitmap 高效判断是否是一人一单
     * <p>
     * 当 userId 比较长的时候，bitmap 的效率会降低。因此 userId 长的时候，不推荐使用此方法。
     * userId 长的时候，可以使用 Set 来代替。参见 {@link #isOnePersonOneOrderBySet(Long)}
     */
    private boolean isOnePersonOneOrderByBitmap(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        String userVoucherKey = "user_voucher:" + voucherId;

        //获取该用户在Bitmaps中的状态
        Boolean isOrder = stringRedisTemplate.opsForValue().getBit(userVoucherKey, userId);
        if (isOrder == null || !isOrder) {
            //如果该用户没有购买过该商品，那么在Bitmaps中设置该用户的状态为已购买
            stringRedisTemplate.opsForValue().setBit(userVoucherKey, userId, true);
            return true;
        } else {
            //如果该用户已经购买过该商品，那么返回false
            return false;
        }
    }

}
