package org.datanerd.dataflow.wikipedia;


import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author ivan
 */
public class ExtractJsonFnTest {

  @Rule
  public TestPipeline p = TestPipeline.create();

  @Test
  public void testExtractJson() {
    PCollection<String> output = p
        .apply(Create.of("data {1:a}", "data {2:b}"))
        .apply(ParDo.of(new ExtractJsonFn()));
    PAssert.that(output).containsInAnyOrder("{1:a}","{2:b}");
    p.run().waitUntilFinish();
  }
}