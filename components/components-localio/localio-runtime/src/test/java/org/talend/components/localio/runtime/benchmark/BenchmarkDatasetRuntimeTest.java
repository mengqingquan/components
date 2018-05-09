package org.talend.components.localio.runtime.benchmark;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.talend.components.localio.runtime.benchmark.BenchmarkInputRuntimeTest.createComponentProperties;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.IndexedRecord;
import org.junit.Test;
import org.talend.components.localio.benchmark.BenchmarkDatasetProperties;
import org.talend.daikon.java8.Consumer;

public class BenchmarkDatasetRuntimeTest {

    @Test
    public void testNexmarkSample() {
        final BenchmarkDatasetProperties props = createComponentProperties().getDatasetProperties();
        props.generator.setValue(BenchmarkDatasetProperties.GeneratorType.NEXMARK);

        BenchmarkDatasetRuntime runtime = new BenchmarkDatasetRuntime();
        runtime.initialize(null, props);

        // Get the two records.
        final List<IndexedRecord> consumed = new ArrayList<>();
        runtime.getSample(100, new Consumer<IndexedRecord>() {

            @Override
            public void accept(IndexedRecord ir) {
                consumed.add(ir);
            }
        });

        assertThat(consumed, hasSize(100));
    }

}