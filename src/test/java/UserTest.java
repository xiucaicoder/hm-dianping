import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import com.hmdp.HmDianPingApplication;
import com.hmdp.dto.UserDTO;
import com.hmdp.service.impl.UserServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;


/**
 * @author zxl
 * @date 2023/12/2 18:15
 * @description 生成测试用户的测试类
 */
@SpringBootTest(classes = HmDianPingApplication.class)
public class UserTest {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Test
    public void testUserLogin() {
        //生成 100 个测试用户
        for (int i = 0; i < 100; i++) {
            //生成token
            String token = UUID.randomUUID().toString(true);
            //控制台打印 token，自己新建一个 tokens.txt 文件，把这个 token 复制进去
            System.out.println(token);

            //模拟用户登录
            UserDTO userDTO = new UserDTO();
            Long userId = getRandomUserId(6);
            userDTO.setId(userId);
            userDTO.setNickName("测试用户 " + userId);
            userDTO.setIcon("https://www.baidu.com");

            Map<String, Object> map = BeanUtil.beanToMap(userDTO, new HashMap<>()
                    , CopyOptions.create().setIgnoreNullValue(true)
                            .setFieldValueEditor(
                                    (name, value) -> value.toString()
                            ));

            //保存用户信息到redis
            stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY + token, map);
            //设置过期时间
            stringRedisTemplate.expire(LOGIN_USER_KEY + token, LOGIN_USER_TTL, TimeUnit.MINUTES);
        }
    }

    /**
     * 生成随机用户id
     *
     * @param length 长度
     * @return 用户id
     */
    private Long getRandomUserId(int length) {
        long lowerBound = (long) Math.pow(10, length - 1);
        long upperBound = (long) Math.pow(10, length) - 1;

        return lowerBound + ThreadLocalRandom.current().nextLong(upperBound - lowerBound + 1);
    }
}