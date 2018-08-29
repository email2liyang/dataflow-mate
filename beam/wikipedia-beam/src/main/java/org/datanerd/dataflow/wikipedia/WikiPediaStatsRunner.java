package org.datanerd.dataflow.wikipedia;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * @author ivan
 */
public class WikiPediaStatsRunner {

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
        .apply(ParDo.of(new ExtractJsonFn()));

    pipeline.run();
  }
}
