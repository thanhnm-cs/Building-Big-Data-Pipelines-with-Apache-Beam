/**
 * Copyright 2021-2024 Packt Publishing Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.packtpub.beam.chapter2;

import static com.packtpub.beam.chapter2.SlidingWindowWordLength.calculateAverageWordLength;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

public class SlidingWindowWordLengthTest {

  @Test
  public void testAverageWordCalculation() {
    Instant now = Instant.now();
    // round this to one minute boundary
    now = now.plus(-now.getMillis() % 60000);
    Pipeline pipeline = Pipeline.create();
    PCollection<String> input =
        pipeline.apply(
            TestStream.create(StringUtf8Coder.of())
                .addElements(TimestampedValue.of("a", now))
                .addElements(TimestampedValue.of("bb", now.plus(1999)))
                // add different slide
                .addElements(TimestampedValue.of("ccc", now.plus(5000)))
                // first non-overlapping set of windows
                .addElements(TimestampedValue.of("dddd", now.plus(10000)))
                .advanceWatermarkToInfinity());
    PCollection<Double> averages = calculateAverageWordLength(input);
    PAssert.that(averages).containsInAnyOrder(1.5, 1.5, 2.0, 2.0, 2.0, 3.5, 3.5, 4.0, 4.0, 4.0);
    pipeline.run();
  }
}
