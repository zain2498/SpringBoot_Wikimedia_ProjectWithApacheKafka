package net.javaguideszain1.kafka;

import com.launchdarkly.eventsource.ErrorStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.HttpConnectStrategy;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import net.javaguideszain1.handler.WikimediaChangesHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaChangesProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaChangesProducer.class);
    private KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws InterruptedException {

        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate, "wikimedia_recentchange");
        URI url = URI.create("https://stream.wikimedia.org/v2/stream/recentchange");

        HttpConnectStrategy connectStrategy = HttpConnectStrategy
                .http(url).connectTimeout(5, TimeUnit.SECONDS);

        EventSource.Builder eventBuilder = new EventSource.Builder(connectStrategy)
                .retryDelay(2, TimeUnit.SECONDS)
                .errorStrategy(ErrorStrategy.alwaysContinue());

        BackgroundEventSource.Builder backgroundBuilder = new BackgroundEventSource.Builder(eventHandler, eventBuilder);
        BackgroundEventSource eventSource = backgroundBuilder.build();
        eventSource.start();
        try {
            TimeUnit.MINUTES.sleep(10);
        } finally {
            eventSource.close();
        }
    }
}
