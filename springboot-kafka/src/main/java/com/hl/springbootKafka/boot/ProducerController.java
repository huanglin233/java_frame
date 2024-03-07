package com.hl.springbootKafka.boot;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * kafka消息生产者,基于springboot
 */
@RestController
public class ProducerController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerController.class);

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/send/{msg}")
    public String sendMsg(@PathVariable("msg") String msg) throws InterruptedException, ExecutionException {
            ListenableFuture<SendResult<String,String>> send = kafkaTemplate.send("test", msg);
            SendResult<String,String> sendResult = send.get();
            RecordMetadata recordMetadata = sendResult.getRecordMetadata();
            LOGGER.info(recordMetadata.serializedKeySize() + "");
            LOGGER.info(recordMetadata.serializedValueSize() + "");

            return recordMetadata.toString();
    }

    @RequestMapping("/streams")
    public String sendMessageStreams(){
        kafkaTemplate.send("test","key1","the the the the value hello");
        return "success";
    }
}