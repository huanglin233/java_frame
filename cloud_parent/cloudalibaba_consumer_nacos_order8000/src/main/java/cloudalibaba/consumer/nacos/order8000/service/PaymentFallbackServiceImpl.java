package cloudalibaba.consumer.nacos.order8000.service;

import org.springframework.stereotype.Component;

import com.hl.cloud.entities.Payment;
import com.hl.cloud.utils.CommonResult;

@Component
public class PaymentFallbackServiceImpl implements PaymentService {

    @Override
    public CommonResult<Payment> getPaymentById(Long id) {
        return new CommonResult<Payment>(444, "服务降级返回,----PaymentFallbackService");
    }
}