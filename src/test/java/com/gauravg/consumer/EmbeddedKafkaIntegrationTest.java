package com.gauravg.consumer;

import com.gauravg.demo.RequestReplyKafkaApplication;
import com.gauravg.model.Model;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.requestreply.CorrelationKey;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;

import java.math.BigInteger;
import java.util.UUID;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = RequestReplyKafkaApplication.class)
@DirtiesContext
@ContextConfiguration
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class EmbeddedKafkaIntegrationTest {

    @Autowired
    ReplyingKafkaTemplate<String, Model, Model> kafkaTemplate;

    @Value("${kafka.topic.request-topic}")
    private String topic;

    @Value("${kafka.topic.requestreply-topic}")
    String requestReplyTopic;

    @Test
    public void givenEmbeddedKafkaBroker_whenSendingtoSimpleProducer_thenMessageReceived() throws Exception {

        Model model = new Model();
        model.setFirstNumber(1);
        model.setSecondNumber(2);
        model.setAdditionalProperty("test", "value");
        ProducerRecord<String, Model> record = new ProducerRecord<>(topic, model);
        // set reply topic in header
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic.getBytes()));
        // post in kafka topic
        RequestReplyFuture<String, Model, Model> sendAndReceive = kafkaTemplate.sendAndReceive(record);

        // confirm if producer produced successfully
        SendResult<String, Model> sendResult = sendAndReceive.getSendFuture().get();

        //print all headers
        sendResult.getProducerRecord().headers().forEach(header -> System.out.println("### Producer: " + header.key() + ":" + new BigInteger(header.value())));
        System.out.println("### Producer Value: " + sendResult.getProducerRecord().value());
        System.out.println("### Producer key: " + sendResult.getProducerRecord().key());
        System.out.println("### Producer topic: " + sendResult.getProducerRecord().topic());

        // get consumer record
        ConsumerRecord<String, Model> consumerRecord = sendAndReceive.get();
        consumerRecord.headers().forEach(header -> System.out.println("### " + header.key() + ":" + new BigInteger(header.value())));
        System.out.println("### Consumer Value: " + consumerRecord.value());
        System.out.println("### Consumer Topic: " + consumerRecord.topic());
        System.out.println("### Consumer key: " + consumerRecord.key());
    }

    @Test
    public void test_send() throws Exception {

        Model model = new Model();
        model.setFirstNumber(1);
        model.setSecondNumber(2);
        model.setAdditionalProperty("test", "value");
        ProducerRecord<String, Model> record = new ProducerRecord<>(topic, model);
        // set reply topic in header
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic.getBytes()));
        // post in kafka topic
        ListenableFuture<SendResult<String, Model>> send = kafkaTemplate.send(topic, model);

        SendResult<String, Model> stringModelSendResult = send.get();
        System.out.println(stringModelSendResult.getRecordMetadata());
        System.out.println(stringModelSendResult.getProducerRecord().headers());
        System.out.println(stringModelSendResult.getProducerRecord().topic());
        // confirm if producer produced successfully
    }
}
