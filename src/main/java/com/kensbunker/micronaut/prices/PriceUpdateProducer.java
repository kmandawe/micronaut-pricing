package com.kensbunker.micronaut.prices;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.reactivex.Flowable;
import java.util.List;
import org.apache.kafka.clients.producer.RecordMetadata;

@KafkaClient(batch = true)
public interface PriceUpdateProducer {

  @Topic("price_update")
  Flowable<RecordMetadata> send(List<PriceUpdate> prices);
}
