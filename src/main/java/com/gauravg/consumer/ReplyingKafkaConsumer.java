package com.gauravg.consumer;

import com.gauravg.model.Model;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;


@Component
public class ReplyingKafkaConsumer {

    @KafkaListener(topics = "${kafka.topic.request-topic}")
    @SendTo
    public Model listen(Model request) throws InterruptedException {

        int sum = request.getFirstNumber() + request.getSecondNumber();
        request.setAdditionalProperty("sum", sum);
        return request;
    }

}
