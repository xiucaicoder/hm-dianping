package com.hmdp.constant;


/**
 * @author zhangxiulin
 * @date 2023/12/1 16:55
 * @description Redis key 常量
 */
public class RedisKeyConstant {
    /**
     * 优惠券秒杀 key
     */
    public static final String SEC_KILL_KEY = "lock:seckill:%s";

    /**
     * 优惠券 key
     */
    public static final String VOUCHER_KEY = "voucher:%s";

}
