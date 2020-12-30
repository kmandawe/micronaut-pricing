package com.kensbunker.micronaut.quotes.external;

import com.kensbunker.micronaut.prices.PriceUpdate;
import com.kensbunker.micronaut.prices.PriceUpdateProducer;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KafkaListener(
    clientId = "mn-pricing-external-quote-consumer",
    groupId = "external-quote-consumer",
    batch = true,
    offsetReset = OffsetReset.EARLIEST)
public class ExternalQuoteConsumer {

  private static Logger LOG = LoggerFactory.getLogger(ExternalQuoteConsumer.class);

  public ExternalQuoteConsumer(PriceUpdateProducer priceUpdateProducer) {
    this.priceUpdateProducer = priceUpdateProducer;
  }

  private final PriceUpdateProducer priceUpdateProducer;

  @Topic("external-quotes")
  void receive(List<ExternalQuote> externalQuotes) {
    LOG.debug("Consuming batch of external quotes {}", externalQuotes);
    // Forward price updates
    final List<PriceUpdate> priceUpdates =
        externalQuotes.stream()
            .map(quote -> new PriceUpdate(quote.getSymbol(), quote.getLastPrice()))
            .collect(Collectors.toList());
    priceUpdateProducer
        .send(priceUpdates)
        .doOnError(e -> LOG.error("Failed to produce: ", e.getCause()))
        .forEach(
            recordMetadata -> {
              LOG.debug(
                  "Record sent to topic {} on offset {}",
                  recordMetadata.topic(),
                  recordMetadata.offset());
            });
  }
}
