package com.kensbunker.micronaut.quotes.external;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KafkaListener(
    clientId = "mn-pricing-external-quote-consumer",
    groupId = "external-quote-consumer",
    batch = true)
public class ExternalQuoteConsumer {

  private static Logger LOG = LoggerFactory.getLogger(ExternalQuoteConsumer.class);

  @Topic("external-quotes")
  void receive(List<ExternalQuote> externalQuotes) {
    LOG.debug("Consuming batch of external quotes {}", externalQuotes);
  }
}
