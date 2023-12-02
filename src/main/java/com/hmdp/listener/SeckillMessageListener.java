package com.hmdp.listener;

import com.alibaba.fastjson.JSON;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Map;


/**
 * @author zhangxiulin
 * @date 2023/11/30 16:59
 * @description 秒杀消息监听器
 */
@Slf4j
@Service
public class SeckillMessageListener {

    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @RabbitListener(queuesToDeclare = @Queue(value = "seckillQueue", durable = "true"))
    public void receiveSeckillMessage(Map<String, String> message) {
        log.warn("接收到秒杀信息：{}", JSON.toJSON(message));

        String userId = message.get("userId");
        String voucherId = message.get("voucherId");
        String orderId = message.get("orderId");

        //扣减库存
        this.deductStock(Long.valueOf(voucherId));

        //创建用户订单
        this.createOrder(Long.valueOf(userId), Long.valueOf(voucherId), Long.valueOf(orderId));
    }

    /**
     * 扣减库存
     */
    private void deductStock(Long voucherId) {

        //扣减数据库库存
        boolean dbFlag = deductDbStock(voucherId);

        if (!dbFlag) {
            //飞书群消息警告，人工干预
            log.error("扣减库存失败，优惠券id：{}", voucherId);
        }
    }

    /**
     * 扣减数据库库存
     */
    private boolean deductDbStock(Long voucherId) {
        //扣减数据库库存
        return seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .update();
    }

    private void createOrder(Long userId, Long voucherId, Long orderId) {
        try {
            VoucherOrder voucherOrder = new VoucherOrder();

            voucherOrder.setId(orderId);
            voucherOrder.setUserId(userId);
            voucherOrder.setVoucherId(voucherId);

            voucherOrderService.save(voucherOrder);
        } catch (Exception e) {
            log.error("秒杀失败，原因：{}", e.getMessage());
            //将失败的订单信息存入数据表里，方便后面排查问题
        }
    }
}
