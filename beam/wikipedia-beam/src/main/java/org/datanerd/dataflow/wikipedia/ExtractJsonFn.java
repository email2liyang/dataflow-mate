package org.datanerd.dataflow.wikipedia;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author ivan
 */
public class ExtractJsonFn extends DoFn<String, String> {

  private transient Pattern pattern;

  @Setup
  public void setUp() {
    pattern = Pattern.compile("data (.*)");
  }

  @ProcessElement
  public void processElement(@Element String element, OutputReceiver<String> receiver) {
    Matcher matcher = pattern.matcher(element);
    if (matcher.find()) {
      receiver.output(matcher.group());
    }
  }
}
