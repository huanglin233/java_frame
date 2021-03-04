package com.hl.cloud.payment8005.controller;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.hl.cloud.payment8005.service.PaymentService;
import com.netflix.hystrix.contrib.javanica.annotation.HystrixCommand;
import com.netflix.hystrix.contrib.javanica.annotation.HystrixProperty;

import cn.hutool.core.util.IdUtil;

@RestController
public class PaymentController {
    private static final Logger logger = LoggerFactory.getLogger(PaymentController.class); 

    @Resource
    private PaymentService paymentService;

    @Value("${server.port}")
    private String serverPort;

    @HystrixCommand
    @GetMapping("/payment/hystrix/ok/{id}")
    public String paymentInfo_ok(@PathVariable("id") Integer id) {
        String paymentInfo_ok = paymentService.paymentInfo_ok(id);
        logger.info("-----Result : " + paymentInfo_ok);

        return paymentInfo_ok;
    }

    @HystrixCommand(fallbackMethod = "paymentTimeoutHandler", commandProperties = {
            @HystrixProperty(name = "execution.isolation.thread.timeoutInMilliseconds", value="3000")})
    @GetMapping("/payment/hystrix/timeout/{id}")
    public String paymentInfoTimeOut(@PathVariable("id") Integer id) {
        String paymentInfo_TimeOut = paymentService.paymentInfo_TimeOut(id);
        logger.info("-----Result : " + paymentInfo_TimeOut);

        return paymentInfo_TimeOut;
    }

    // ====服务熔断
    @HystrixCommand(fallbackMethod = "paymentInfo_CircuitBreaker", commandProperties = {
            @HystrixProperty(name = "execution.isolation.strategy", value = "THREAD"), // 设置隔离策略,THREAD 表示线程池 SEMAPHOR 信号池
            @HystrixProperty(name = "execution.isolation.semaphore.maxConcurrentRequests", value = "10"), // 当隔离策略选择信号池的时候,用来设置信号池的大小(最大并发数据)
            @HystrixProperty(name = "execution.isolation.thread.timeoutInMilliseconds", value = "10"), // 配置命令执行的超时时间
            @HystrixProperty(name = "execution.timeout.enabled", value = "true"), // 是否启用超时时间
            @HystrixProperty(name = "execution.isolation.thread.interruptOnTimeout", value = "true"), // 执行超时时是否中断
//            @HystrixProperty(name = "execution.isolation.thread.interruptOnFutureCancel", value = "true"), // 执行被取消时是否中断
            @HystrixProperty(name = "fallback.isolation.semaphore.maxConcurrentRequests", value = "10"), // 允许回调方法执行的最大并发数
            @HystrixProperty(name = "fallback.enabled", value = "true"), // 服务降级是否启用,是否执行回调函数
            @HystrixProperty(name = "circuitBreaker.enabled", value ="true"), //是否开启断路路由
            @HystrixProperty(name = "circuitBreaker.requestVolumeThreshold", value = "10"), // 请求次数,并发超过这个次数,开启短路器
            @HystrixProperty(name = "circuitBreaker.sleepWindowInMilliseconds", value = "1000"), // 时间窗口
            @HystrixProperty(name = "circuitBreaker.errorThresholdPercentage", value = "60"), // 失败率达到多少跳闸
            @HystrixProperty(name = "circuitBreaker.forceOpen", value = "false"), // 断路器强制打开
            @HystrixProperty(name = "metrics.rollingStats.timeInMilliseconds", value = "1000"), // 滚动时间窗设置,该时间用于断路由器判断健康是需要收集信息的持续时间
            @HystrixProperty(name = "metrics.rollingStats.numBuckets", value = "10"), // 该属性用来设置滚动时间窗统计指标信息时划分"桶"的数量
            @HystrixProperty(name = "metrics.rollingPercentile.enabled", value = "false"), // 该属性用来设置对命令执行的延迟是否使用百分位数来跟踪和计算,如果设置为false,那么所有的概率要统计都将返回 -1
            @HystrixProperty(name = "metrics.rollingPercentile.timeInMilliseconds", value = "600000"), //该属性用来设置百分位统计的滚动窗口的持续时间,单位为毫秒
            @HystrixProperty(name = "metrics.rollingPercentile.numBuckets", value = "60000"), // 该属性用于设置百分位统计滚动窗口中使用"桶"的数量
            @HystrixProperty(name = "metrics.rollingPercentile.bucketSize", value = "100"), // 该属性用来设置在执行过程中每个"桶"中保留的最大执行次数,如果在滚动时间窗口内发生超过该设定值的执行次数,就出最初的位置开始重写.
            @HystrixProperty(name = "metrics.healthSnapshot.intervalInMilliseconds", value = "500"), // 该属性用来设置采集影响断路器状态的健康快照(请求成功,错误百分比)的间隔等待时间
            @HystrixProperty(name = "requestCache.enabled", value = "true"), // 是否开启请求缓存
            @HystrixProperty(name = "requestLog.enabled", value = "true"), // HystrixCommand的执行和事件是否打印日志到HystrixRequestLong中
    }, threadPoolProperties = {
            @HystrixProperty(name = "coreSize", value = "5"), // 该参数用来设置执行命令线程池的核心线程数,该值也就是命令执行的最大并发值
            @HystrixProperty(name = "maxQueueSize", value = "5"), // 该参数用来设置线程池队列的大小,当设置为-1时,线程池将使用SynchronousQueue实现队列,否则使用LinkedBlockingQueue实现队列
            @HystrixProperty(name = "queueSizeRejectionThreshold", value = "10"), // 该参数用来为队列设置阈值,通过该参数,即使队列没有达到最大值也能拒绝请求,该参数最要是对LinkedBlockingQueue
    })
    @GetMapping("/payment/hystrix/circuitBreaker/{id}")
    public String getCircutBreaker(@PathVariable("id") Integer id) {
        if(id < 0) {
            throw new RuntimeException("-----Id不能为空");
        }
        String serialNumber = IdUtil.simpleUUID();

        return Thread.currentThread().getName() + "\t" + "调用成功,流水号为: " + serialNumber;
    }

    public String paymentTimeoutHandler(Integer id) {
        return "兜底线程池:" + Thread.currentThread().getName() + " paymentTimeoutHandler,id: " + id + "\t" + "~~~~(>_<)~~~~";
    }

    public String paymentInfo_CircuitBreaker(@PathVariable("id") Integer id) {
        return "paymentInfo_CircutBreaker,服务已经断熔";
    }
}