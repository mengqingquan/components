// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.localio.runtime.benchmark;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.talend.components.adapter.beam.BeamLocalRunnerOption;
import org.talend.components.adapter.beam.transform.DirectConsumerCollector;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.common.dataset.runtime.DatasetRuntime;
import org.talend.components.localio.LocalIOErrorCode;
import org.talend.components.localio.benchmark.BenchmarkDatasetProperties;
import org.talend.components.localio.benchmark.BenchmarkInputProperties;
import org.talend.daikon.exception.TalendRuntimeException;
import org.talend.daikon.java8.Consumer;
import org.talend.daikon.properties.ValidationResult;

public class BenchmarkDatasetRuntime implements DatasetRuntime<BenchmarkDatasetProperties> {

    /**
     * The dataset instance that this runtime is configured for.
     */
    private BenchmarkDatasetProperties properties = null;

    @Override
    public ValidationResult initialize(RuntimeContainer container, BenchmarkDatasetProperties properties) {
        this.properties = properties;
        return ValidationResult.OK;
    }

    @Override
    public Schema getSchema() {
        switch (properties.generator.getValue()) {
        case NEXMARK:
            return NexmarkGenerator.EventIndexedRecord.SCHEMA;
        default:
            throw LocalIOErrorCode.createCannotParseSchema(null, properties.generator.getValue().name());
        }
    }

    @Override
    public void getSample(int limit, Consumer<IndexedRecord> consumer) {
        for (IndexedRecord value : getValues(limit)) {
            consumer.accept(value);
        }
    }

    private List<IndexedRecord> getValues(int limit) {
        final List<IndexedRecord> values = new ArrayList<>();

        DirectOptions options = BeamLocalRunnerOption.getOptions();
        final Pipeline p = Pipeline.create(options);

        BenchmarkInputProperties inputProperties = new BenchmarkInputProperties(null);
        inputProperties.setDatasetProperties(properties);
        inputProperties.init();

        inputProperties.isStreaming.setValue(false);
        inputProperties.maxNumRecords.setValue((long) limit);

        try (DirectConsumerCollector<IndexedRecord> collector = DirectConsumerCollector.of(new Consumer<IndexedRecord>() {

            @Override
            public void accept(IndexedRecord r) {
                values.add(r);
            }
        })) {
            // Collect a sample of the input records.
            switch (properties.generator.getValue()) {
            case NEXMARK:
                NexmarkGenerator.expand(p.begin(), inputProperties).apply(collector);
                break;
            default:
                throw LocalIOErrorCode.createCannotParseSchema(null, properties.generator.getValue().name());
            }
            try {
                p.run().waitUntilFinish();
            } catch (Pipeline.PipelineExecutionException e) {
                if (e.getCause() instanceof TalendRuntimeException)
                    throw (TalendRuntimeException) e.getCause();
                throw e;
            }
        }

        return values;
    }
}
