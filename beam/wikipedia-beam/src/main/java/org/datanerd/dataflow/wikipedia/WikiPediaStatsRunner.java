package org.datanerd.dataflow.wikipedia;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ivan
 */
public class WikiPediaStatsRunner {

  private static Logger log = LoggerFactory.getLogger(WikiPediaStatsRunner.class); //NOPMD
  public static void main(String[] args) {
    final StreamingOptions options =
        PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(StreamingOptions.class);
    options.setStreaming(true);
    final Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Read.from(new WikiPediaRecentChangeSource()))
        .apply(Filter.by(line -> line.startsWith("data")))
        .apply(ParDo.of(new ExtractJsonFn()))
        .apply(MapElements.via(new SimpleFunction<String, KV<Boolean,String>>() {
          @Override
          public KV<Boolean,String> apply(String json) {

            JsonElement jsonElement = new JsonParser().parse(json);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            String prettyJson = gson.toJson(jsonElement);
            boolean isBot = ((JsonObject)jsonElement).get("bot").getAsBoolean();
            return KV.of(isBot, prettyJson);
          }
        }))
    .apply(Window.into(FixedWindows.of(Duration.standardSeconds(20))))
    .apply(GroupByKey.create())
    .apply(MapElements.via(new SimpleFunction<KV<Boolean,Iterable<String>>, KV<Boolean,Long>>() {
      @Override
      public KV<Boolean, Long> apply(KV<Boolean, Iterable<String>> input) {
        Long counter = 0L;
        for(String json: input.getValue()){
          counter = counter + 1;
        }
        return KV.of(input.getKey(), counter);
      }
    }))
    .apply(MapElements.via(new SimpleFunction<KV<Boolean,Long>,Long>(){
      @Override
      public Long apply(KV<Boolean, Long> input) {
        log.info(" bot -> {}, count -> {}",input.getKey(),input.getValue());
        return input.getValue();
      }
    }));

    pipeline.run();
  }
}
