package com.kensbunker.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.kensbunker.micronaut.prices.PriceUpdate;
import com.kensbunker.micronaut.quotes.external.ExternalQuote;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.core.util.StringUtils;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class TestExternalQuoteConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(TestExternalQuoteConsumer.class);
  private static final String PROPERTY_NAME = "TestExternalQuoteConsumer";
  private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

  @Rule public static KafkaContainer kafka = new KafkaContainer();

  private static ApplicationContext context;

  @BeforeAll
  static void start() {
    kafka.start();
    LOG.debug("Bootstrap Servers: {}", kafka.getBootstrapServers());

    context =
        ApplicationContext.run(
            CollectionUtils.mapOf(
                "kafka.bootstrap.servers",
                kafka.getBootstrapServers(),
                PROPERTY_NAME,
                StringUtils.TRUE),
            Environment.TEST);
  }

  @AfterAll
  static void stop() {
    kafka.stop();
    context.close();
  }

  @Test
  void consumingPriceUpdatesWorksCorrectly() {
    final TestScopedExternalQuoteProducer testProducer =
        context.getBean(TestScopedExternalQuoteProducer.class);
    IntStream.range(0, 4)
        .forEach(
            count -> {
              testProducer.send(
                  "TEST" + count,
                  new ExternalQuote(
                      "TEST" + count,
                      randomValue(RANDOM),
                      randomValue(RANDOM)));
            });
    PriceUpdateObserver observer =
        TestExternalQuoteConsumer.context.getBean(PriceUpdateObserver.class);

    Awaitility.await()
        .untilAsserted(
            () -> {
              assertEquals(4, observer.inspected.size());
            });
  }

  private BigDecimal randomValue(ThreadLocalRandom random) {
    return BigDecimal.valueOf(random.nextDouble(0, 1000));
  }

  @KafkaListener(clientId = "price-update-observer", offsetReset = OffsetReset.EARLIEST)
  @Requires(env = Environment.TEST)
  @Requires(property = PROPERTY_NAME, value = StringUtils.TRUE)
  public static class PriceUpdateObserver {
    List<PriceUpdate> inspected = new ArrayList<>();

    @Topic("price_update")
    void receive(List<PriceUpdate> priceUpdates) {
      LOG.debug("Consumed: {}", priceUpdates);
      inspected.addAll(priceUpdates);
    }
  }

  @KafkaClient
  @Requires(env = Environment.TEST)
  @Requires(property = PROPERTY_NAME, value = StringUtils.TRUE)
  public interface TestScopedExternalQuoteProducer {

    @Topic("external-quotes")
    void send(@KafkaKey String symbol, ExternalQuote externalQuote);
  }
}
