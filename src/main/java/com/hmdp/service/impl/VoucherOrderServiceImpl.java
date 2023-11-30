package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.RedissonDistributedLocker;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;

import static com.hmdp.constant.CommonConstant.LOCK_WAIT_TIME;
import static com.hmdp.constant.CommonConstant.MAX_LEASE_TIME;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
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

    @Override
    public Result seckillVoucher(Long voucherId) {
        String lockKey = "lock:seckill:" + voucherId;
        Long orderId;

        try {
            boolean getLock = locker.tryLock(lockKey, LOCK_WAIT_TIME, MAX_LEASE_TIME);
            if (!getLock) {
                //获取锁失败
                return Result.fail("活动火爆，请刷新重试！");
            }

            //查询优惠券
            SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
            if (!isOnePersonOneOrderByDb(voucherId)) {
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

            //判断库存是否充足并扣减库存
            if (!deductStock(voucherId)) {
                // 库存不足
                return Result.fail("库存不足！");
            }

            //创建订单
            orderId = createOrder(voucherId);
        } finally {
            locker.unlock(lockKey);
        }

        return Result.ok(orderId);
    }

    private boolean isSeckillStarted(SeckillVoucher voucher) {
        return voucher.getBeginTime().isBefore(LocalDateTime.now());
    }

    private boolean isSeckillEnded(SeckillVoucher voucher) {
        return voucher.getEndTime().isBefore(LocalDateTime.now());
    }

    private boolean deductStock(Long voucherId) {
        return seckillVoucherService.update()
                .setSql("stock= stock -1")
                .eq("voucher_id", voucherId)
                .update();
    }

    private Long createOrder(Long voucherId) {
        VoucherOrder voucherOrder = new VoucherOrder();
        //订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //用户id
        Long userId = UserHolder.getUser().getId();
        voucherOrder.setUserId(userId);
        //代金券id
        voucherOrder.setVoucherId(voucherId);
        super.save(voucherOrder);

        return orderId;
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
     */
    private boolean isOnePersonOneOrderByRedis(Long voucherId) {
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
