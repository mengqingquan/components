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

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.talend.components.adapter.beam.transform.DirectCollector;
import org.talend.components.localio.benchmark.BenchmarkDatasetProperties;
import org.talend.components.localio.benchmark.BenchmarkDatastoreProperties;
import org.talend.components.localio.benchmark.BenchmarkInputProperties;

/**
 * Unit tests for {@link BenchmarkInputRuntime}.
 */
public class BenchmarkInputRuntimeTest {

    /**
     * @return the properties for this component fully initialized with the default values.
     */
    public static BenchmarkInputProperties createComponentProperties() {
        BenchmarkDatastoreProperties datastoreProps = new BenchmarkDatastoreProperties(null);
        datastoreProps.init();
        BenchmarkDatasetProperties datasetProps = new BenchmarkDatasetProperties(null);
        datasetProps.init();
        datasetProps.setDatastoreProperties(datastoreProps);
        BenchmarkInputProperties componentProps = new BenchmarkInputProperties(null);
        componentProps.init();
        componentProps.setDatasetProperties(datasetProps);
        return componentProps;
    }

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testPredefinedNexmark() {
        // The component properties to test.
        BenchmarkInputProperties props = createComponentProperties();
        props.getDatasetProperties().generator.setValue(BenchmarkDatasetProperties.GeneratorType.NEXMARK);
        props.isStreaming.setValue(false);
        props.maxNumRecords.setValue(2L);
        props.useSeed.setValue(true);
        props.seed.setValue(0L);

        BenchmarkInputRuntime runtime = new BenchmarkInputRuntime();
        runtime.initialize(null, props);

        PCollection<IndexedRecord> indexRecords = pipeline.apply(runtime);
        try (DirectCollector<IndexedRecord> collector = DirectCollector.of()) {
            indexRecords.apply(collector);

            // Run the pipeline to fill the collectors.
            pipeline.run().waitUntilFinish();

            // Validate the contents of the collected outputs.
            List<IndexedRecord> outputs = collector.getRecords();
            assertThat(outputs, hasSize(2));
            IndexedRecord r1 = outputs.get(0);
            IndexedRecord r2 = outputs.get(1);

            // Check the schema and contents.
            assertThat(r1.getSchema().getFields(), hasSize(3));
            assertThat(r1.getSchema().getFields().get(0).name(), is("auction"));
            assertThat(r1.getSchema().getFields().get(0).schema().getType(), is(Schema.Type.UNION));
            assertThat(r1.getSchema().getFields().get(1).name(), is("bid"));
            assertThat(r1.getSchema().getFields().get(1).schema().getType(), is(Schema.Type.UNION));
            assertThat(r1.getSchema().getFields().get(2).name(), is("person"));
            assertThat(r1.getSchema().getFields().get(2).schema().getType(), is(Schema.Type.UNION));

            // The first record is a person: {"id": 1000, "name": "Peter Jones", "emailAddress": "nhd@xcat.com", ...
            assertThat(r1.get(0), nullValue());
            assertThat(r1.get(1), nullValue());
            assertThat(r1.get(2), instanceOf(IndexedRecord.class));
            assertThat(((IndexedRecord) r1.get(2)).get(0), is((Object) 1000L));
            assertThat(((IndexedRecord) r1.get(2)).get(1).toString(), is("Peter Jones"));

            // The second record is an auction: {"id": 1000, "itemName": "wkx mgee", ..., "initialBid": 28873...
            assertThat(r2.get(0), instanceOf(IndexedRecord.class));
            assertThat(((IndexedRecord) r2.get(0)).get(0), is((Object) 1000L));
            assertThat(((IndexedRecord) r2.get(0)).get(1).toString(), is("wkx mgee"));
            assertThat(r2.get(1), nullValue());
            assertThat(r2.get(2), nullValue());

            // This is the desired format for Event records (A union of records). Not well supported.
            // assertThat(r1.getSchema().getFields(), hasSize(2));
            // assertThat(r1.getSchema().getFields().get(0).name(), is("type"));
            // assertThat(r1.getSchema().getFields().get(0).schema().getType(), is(Schema.Type.ENUM));
            // assertThat(r1.getSchema().getFields().get(1).name(), is("value"));
            // assertThat(r1.getSchema().getFields().get(1).schema().getType(), is(Schema.Type.UNION));

            // assertThat(r1.get(0).toString(), is("1"));
            // assertThat(r1.get(1).toString(), is("one"));
            // assertThat(r2.getSchema(), is(r1.getSchema()));
            // assertThat(r2.get(0).toString(), is("2"));
            // assertThat(r2.get(1).toString(), is("two"));
        }
    }

}
