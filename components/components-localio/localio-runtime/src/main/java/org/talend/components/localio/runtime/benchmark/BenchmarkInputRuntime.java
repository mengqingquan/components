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

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.talend.components.api.component.runtime.RuntimableRuntime;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.localio.LocalIOErrorCode;
import org.talend.components.localio.benchmark.BenchmarkInputProperties;
import org.talend.daikon.properties.ValidationResult;

public class BenchmarkInputRuntime extends PTransform<PBegin, PCollection<IndexedRecord>>
        implements RuntimableRuntime<BenchmarkInputProperties> {

    private BenchmarkInputProperties properties;

    public ValidationResult initialize(RuntimeContainer container, BenchmarkInputProperties componentProperties) {
        this.properties = componentProperties;
        return ValidationResult.OK;
    }

    @Override
    public PCollection<IndexedRecord> expand(PBegin begin) {
        switch (properties.getDatasetProperties().generator.getValue()) {
        case NEXMARK:
            return NexmarkGenerator.expand(begin, properties);
        default:
            throw LocalIOErrorCode.createCannotParseSchema(null, properties.getDatasetProperties().generator.getValue().name());
        }
    }
}
