package com.mina.kafka.reassignment.test.component;

import static java.text.MessageFormat.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mina.kafka.reassignment.test.model.Message;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MessageProducer {

  private static final Logger LOG = LoggerFactory.getLogger(MessageProducer.class);
  private static final String MESSAGE_DESCRIPTION = "Description for message {0}";
  private static final String NEW_MESSAGE_INFO = "A new message was sent to partition {0} on [{1}]. Its offset is: {2}";
  private static final int SLEEP_MILLIS = 2000;
  private static final int MESSAGES_TO_SEND = 100;

  private final ObjectMapper mapper = new ObjectMapper();
  private final String bootstrapServers;
  private final String topicName;

  public MessageProducer(
      @Value("${kafka.bootstrap.servers}") String bootstrapServers,
      @Value("${kafka.topic.name}") String topicName) {
    this.bootstrapServers = bootstrapServers;
    this.topicName = topicName;
  }

  @PostConstruct
  public void produce(){
    Producer<String, String> producer = new KafkaProducer<>(kafkaConfiguration());
    Thread producerRunner = new Thread(() -> {
      try {
        int messageIndex = 0;
        while(messageIndex < MESSAGES_TO_SEND){
          Message msg =  new Message(messageIndex, format(MESSAGE_DESCRIPTION, messageIndex));
          String json = mapper.writeValueAsString(msg);
          Future<RecordMetadata> send = producer.send(new ProducerRecord<>(topicName, json));
          RecordMetadata metadata = send.get();
          if(LOG.isInfoEnabled()){
            LOG.info(format(NEW_MESSAGE_INFO, metadata.partition(), new Date(metadata.timestamp()), metadata.offset()));
          }
          Thread.sleep(SLEEP_MILLIS);
          messageIndex++;
        }
        producer.close();
      } catch (Exception ex) {
        Thread.currentThread().interrupt();
        LOG.error("Error producing messages: "+ex.getLocalizedMessage(), ex);
      }
    });
    producerRunner.start();
  }


  private Properties kafkaConfiguration(){
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return properties;
  }


}
