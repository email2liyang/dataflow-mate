package org.datanerd.dataflow.wikipedia;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

/**
 * @author ivan
 */
class WikiPediaRecentChangeSource extends UnboundedSource<String, UnboundedSource.CheckpointMark> {

  @Override
  public List<? extends UnboundedSource<String, CheckpointMark>> split(int desiredNumSplits, PipelineOptions options)
      throws Exception {
    //only return 1 sources, because it's tcp streams
    List<WikiPediaRecentChangeSource> sources = new ArrayList<>();
    sources.add(new WikiPediaRecentChangeSource());
    return sources;
  }

  @Override
  public UnboundedReader<String> createReader(PipelineOptions options, @Nullable CheckpointMark checkpointMark)
      throws IOException {
    return new WikiPediaRecentChangeReader(this);
  }

  @Override
  public Coder<CheckpointMark> getCheckpointMarkCoder() {
    return null;
  }

  @Override
  public Coder<String> getOutputCoder() {
    return AvroCoder.of(String.class);
  }
}
