package org.datanerd.dataflow.wikipedia;

import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author ivan
 */
class WikiPediaRecentChangeReader extends UnboundedSource.UnboundedReader<String> {

  private static Logger log = LoggerFactory.getLogger(WikiPediaRecentChangeReader.class); //NOPMD
  private final String URL = "https://stream.wikimedia.org/v2/stream/recentchange";
  private final WikiPediaRecentChangeSource outer;
  private BufferedReader bufferedReader;
  private final ConcurrentLinkedQueue<String> queue;
  private String currentLine;

  private ExecutorService threadPool;

  public WikiPediaRecentChangeReader(WikiPediaRecentChangeSource outer) {
    this.outer = outer;
    queue = new ConcurrentLinkedQueue<>();
  }


  @Override
  public boolean start() throws IOException {
    threadPool = Executors.newSingleThreadExecutor();
    threadPool.submit(() -> {
      try {
        HttpClient client = new DefaultHttpClient();
        HttpGet request = new HttpGet(URL);
        HttpResponse response = client.execute(request);
        bufferedReader = new BufferedReader(
            new InputStreamReader(response.getEntity().getContent()));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
          if (StringUtils.isEmpty(line)) {
            //do not process empty line
            continue;
          }
          queue.add(line);
        }
      } catch (IOException e) {
        log.warn(" error on open tcp streams", e);
        throw new IllegalStateException(e);
      }
      System.out.println("thread stop");

    });
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    AtomicLong counter = new AtomicLong();
    while (queue.isEmpty()) {
      try {
        log.debug("queue empty for {}s, system continue to pull the stream"
            , 5 * counter.incrementAndGet());
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    currentLine = queue.poll();
    return currentLine != null;
  }

  @Override
  public String getCurrent() throws NoSuchElementException {
    return currentLine;
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    return Instant.now();
  }

  @Override
  public void close() throws IOException {
    System.out.println("close");
    if (bufferedReader != null) {
      bufferedReader.close();
    }
    if (threadPool != null) {
      threadPool.shutdown();
    }
  }

  @Override
  public Instant getWatermark() {
    return Instant.now();
  }

  @Override
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    return null;
  }

  @Override
  public UnboundedSource<String, ?> getCurrentSource() {
    return outer;
  }
}
