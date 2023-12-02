package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
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
import org.redisson.client.RedisBusyException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        UserDTO user = UserHolder.getUser();
        //获取订单id
        Long orderId = redisIdWorker.nextId("order");
        //执行lua脚本
        Long res = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                user.getId().toString(),
                orderId.toString()
        );

        //判断结果是否为0
        int r = res.intValue();
        if (r != 0) {
            //不为0 没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "禁止重复下单");
        }
        return Result.ok(orderId);
    }

    @PostConstruct
    private void init() {
        CompletableFuture.runAsync(() -> {
            String queueName = "stream.orders";
            String groupName = "g1";
            String consumerName = "c1";

            // 创建 StreamOperations 对象
            StreamOperations<String, Object, Object> streamOps = stringRedisTemplate.opsForStream();

            while (true) {
                try {
                    //检查并创建消费者组
                    try {
                        streamOps.createGroup(queueName, groupName);
                    } catch (RedisSystemException e) {
                        if (!(e.getCause() instanceof RedisBusyException)) {
                            // RedisBusyException 表示消费者组已存在，可以忽略
                        }
                    }

                    // 从消息队列中获取订单信息
                    List<MapRecord<String, Object, Object>> list = streamOps.read(
                            Consumer.from(groupName, consumerName),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    //判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        // 获取失败，没有消息，继续循环
                        continue;
                    }

                    //获取成功，解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //下单
                    handleVoucherOrder(voucherOrder);

                    //ack 确认消息
                    streamOps.acknowledge(queueName, groupName, record.getId());
                } catch (Exception e) {
                    e.printStackTrace();
                    handlePendingList();
                }
            }
        });
    }


    /**
     * 处理待处理的消息(获取到了，但是没有经过 ack 确认的消息)
     */
    private void handlePendingList() {
        String queueName = "stream.orders";
        while (true) {
            try {
                //从消息队列中获取订单信息
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1")
                        , StreamReadOptions.empty().count(1)
                        , StreamOffset.create(queueName, ReadOffset.from("0"))
                );
                //判断消息时候获取成功
                if (list == null || list.isEmpty()) {
                    //获取失败 没有消息 继续循环
                    break;
                }
                //获取成功 解析消息
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> values = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                //下单
                handleVoucherOrder(voucherOrder);
                //ack确认消息
                stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    Thread.sleep(20);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        this.createVoucherOrder(voucherOrder);
    }

    /**
     * 创建订单
     */
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //扣减库存
        boolean isSuccess = seckillVoucherService.update(
                new LambdaUpdateWrapper<SeckillVoucher>()
                        .eq(SeckillVoucher::getVoucherId, voucherOrder.getVoucherId())
                        .gt(SeckillVoucher::getStock, 0)
                        .setSql("stock = stock-1"));
        //创建订单
        this.save(voucherOrder);
    }

}
