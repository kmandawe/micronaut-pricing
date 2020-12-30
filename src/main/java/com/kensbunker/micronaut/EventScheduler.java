package com.kensbunker.micronaut;

import com.kensbunker.micronaut.quotes.external.ExternalQuote;
import com.kensbunker.micronaut.quotes.external.ExternalQuoteProducer;
import io.micronaut.scheduling.annotation.Scheduled;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class EventScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(EventScheduler.class);
  private static final List<String> SYMBOLS = Arrays.asList("AAPL", "AMZN", "FB", "GOOG", "MSFT", "NFLX", "TSLA");
  private final ExternalQuoteProducer externalQuoteProducer;


  public EventScheduler(
    ExternalQuoteProducer externalQuoteProducer) {
    this.externalQuoteProducer = externalQuoteProducer;
  }

  @Scheduled(fixedDelay = "10s")
  void generate() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    final ExternalQuote quote = new ExternalQuote(SYMBOLS.get(random.nextInt(0, SYMBOLS.size() - 1)),
      randomValue(random),
      randomValue(random));

    LOG.debug("Generate external quote {}", quote);
    externalQuoteProducer.send(quote.getSymbol(), quote);
  }

  private BigDecimal randomValue(ThreadLocalRandom random) {
    return BigDecimal.valueOf(random.nextDouble(0, 1000));
  }
}
