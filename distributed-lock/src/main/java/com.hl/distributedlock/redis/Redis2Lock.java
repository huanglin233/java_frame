package com.hl.distributedlock.redis;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;

/**
 * redis获取和删除分布式锁
 *
 * @author huanglin by 2021/5/20
 */
public class Redis2Lock {

    private static final String HOST     = "192.168.56.101";
    private static final int    PORT     = 6379;
    private static final String PASSWORD = "123";

    private static Jedis getJedis() {
        Jedis jedis = new Jedis(HOST, PORT);
//        jedis.auth(PASSWORD);

        return jedis;
    }

    /** 获取Redisson实现的Redis分布式锁(单机模式)*/
    public static RLock getRLock() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.56.101:6379").setDatabase(0);
        // 构造RedissonClient
        RedissonClient redissonClient = Redisson.create(config);
        // 设置锁的资源名称
        RLock dislock = redissonClient.getLock("DISLOCK");

        return dislock;
    }

    /** 获取Redisson实现的Redis分布式锁(集群模式) */
    public static RLock getCRLock() {
        Config config = new Config();
        config.useClusterServers().addNodeAddress("redis://192.168.56.101:7001", "redis://192.168.56.101:7002", "redis://192.168.56.101:7003", "redis://192.168.56.101:7004", "redis://192.168.56.101:7005", "redis://192.168.56.101:7006")
                .setPassword("123456").setConnectTimeout(500).setScanInterval(5000);
        RedissonClient redissonClient = Redisson.create(config);
        RLock crlock = redissonClient.getLock("CRLOCK");

        return crlock;
    }

    /** 通过setnx获取锁 */
    public boolean lock(String key, String value) {
        long lockWaitTimeOut = 3000L;
        long deadTimeLine    = System.currentTimeMillis() + lockWaitTimeOut;

        try {
            Jedis jedis = getJedis();
            for(;;) {
                if(jedis.setnx(key, value) == 1) {
                    return true;
                }

                String currentValue = jedis.get(key);
                // 如果当前时间锁时间失效
                if(currentValue != null && Long.parseLong(currentValue) < System.currentTimeMillis()) {
                    // 获取最近的锁,并更新锁
                    String oldValue = jedis.getSet(key, value);
                    // 如果更新锁时获取到的之前锁和自己持有的过期锁一致，则更新锁时间成功
                    if(oldValue != null && oldValue.equals(currentValue)) {
                        return true;
                    }
                }

                lockWaitTimeOut = deadTimeLine - System.currentTimeMillis();
                // 超过指定时间没有获取到锁，返回失败
                if(lockWaitTimeOut < 0) {
                    return false;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    /** 通过 NX,EX指令获取锁 */
    public boolean lock2(String key, String value) {
        long lockWaitTimeOut = 3000L;
        long deadTimeLine    = System.currentTimeMillis() + lockWaitTimeOut;

        try {
            Jedis jedis = getJedis();

            for(;;) {
                SetParams setParams = new SetParams();
                setParams.nx();
                setParams.px(30000);
                String result = jedis.set(key, value, setParams);

                if("OK".equals(result)) {
                    return true;
                }

                lockWaitTimeOut = System.currentTimeMillis() - deadTimeLine;
                if(lockWaitTimeOut > 0) {
                    System.out.println("-----获取锁等待失败-----");

                    return false;
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    public void unLock(String key, String value) {
        Jedis jedis = getJedis();
        try {
            // 先获取现在redis的锁，版和自己持有的锁是否保持一直
            String currentValue = jedis.get(key);
            if(currentValue != null && value.equals(currentValue)) {
                jedis.del(key);
            }
        } catch (Exception e) {
            System.out.println("unlock error");
        } finally {
            System.out.println(Thread.currentThread().getName() + "-----结束-----");
        }
    }

    public void unLock2(String key, String value) {
        Jedis jedis = getJedis();
        try {
            // 结合lua脚本使其具有原子性
            String luaScript = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
            Object eval = jedis.eval(luaScript, Collections.singletonList(key), Collections.singletonList(value));

            System.out.println("----- eval = " + eval + " -----");
            if("1".equals(eval)) {
                System.out.println("-----释放锁成功-----");
            }
        } catch (Exception e) {
            System.out.println("-----释放锁失败-----");
        }
    }

    public static void main(String[] args) {
        System.out.println("服务正在运行：" + getJedis().ping());
    }
}