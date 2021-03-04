package cloudalibaba.consumer.nacos.order8000.controller;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.hl.cloud.entities.Payment;
import com.hl.cloud.utils.CommonResult;

import cloudalibaba.consumer.nacos.order8000.service.PaymentService;

@RestController
public class NacosOrderController {

    @Resource
    private RestTemplate restTemplate;

    @Resource
    private PaymentService paymentService;

    @Value("${service-url.nacos-user-service}")
    private String serverURL;

    @GetMapping("/consumer/payment/nacos/{id}")
    public String getPayment(@PathVariable("id") Integer id) {
        return restTemplate.getForObject(serverURL + "/payment/nacos/" + id, String.class);
    }

    @GetMapping("/consumer/payment/{id}")
    @SentinelResource(value = "fallback", fallback = "fallbackHandler", blockHandler = "blockHandler", exceptionsToIgnore = {IllegalArgumentException.class})
    public CommonResult<Payment> getPaymentById(@PathVariable("id") Long id) {
//        @SuppressWarnings("unchecked")
//        CommonResult<Payment> payResult= restTemplate.getForObject(serverURL + "/payment/" + id, CommonResult.class, id);
        CommonResult<Payment> payResult = paymentService.getPaymentById(id);
        if(id == 4) {
            throw new IllegalArgumentException("IllegalArgumentException, 非法参数异常");
        } else if(payResult.data == null) {
            throw new NullPointerException("NullPointerException, 没有找到对应id的记录,抛出空指针异常");
        }

        return payResult;
    }

    public CommonResult<Payment> fallbackHandler(Long id, Throwable e){
        Payment payment = new Payment(id, null);

        return new CommonResult<Payment>(444, "fallbackHandler exception = " + e.getMessage(), payment);
    }

    public CommonResult<Payment> blockHandler(Long id, BlockException exception) {
        Payment payment = new Payment(id, null);

        return new CommonResult<Payment>(444, "blockHandler exception = " + exception.getMessage(), payment);
    }
}