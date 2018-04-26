package org.talend.components.simplefileio.runtime;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

/**
 * Unit tests for {@link SimpleRecordFormatCsvIO.ExtractCsvRecord}.
 */
public class ExtractCsvRecordTest {

    /** The DoFn under test. */
    private final DoFnTester<BytesWritable, IndexedRecord> fnBasic = DoFnTester.of(new SimpleRecordFormatCsvIO.ExtractCsvRecord(';', false, "UTF-8", null, null));

    public static String[] toArray(IndexedRecord record) {
        String[] fields = new String[record.getSchema().getFields().size()];
        for (int i = 0; i < fields.length; i++)
            fields[i] = record.get(i).toString();
        return fields;
    }

    public static List<String[]> toArrays(List<IndexedRecord> records) {
        List<String[]> out = new ArrayList<>(records.size());
        for (IndexedRecord r : records) {
            out.add(toArray(r));
        }
        return out;
    }

    public static void assertLine(String msg, DoFnTester<BytesWritable, IndexedRecord> fn, String inputLine, String... expected)
            throws Exception {
        byte[] bytes = inputLine.getBytes("UTF-8");
        
        BytesWritable bsw = new BytesWritable(bytes);
        
        assertThat(msg + ":" + inputLine, toArray(fn.processBundle(bsw).get(0)), arrayContaining(expected));
    }

    @Test
    public void testBasic() throws Exception {
        // Check the DoFn on every possible input line from the examples.
        for (CsvExample csvEx : CsvExample.getCsvExamples()) {
            for (String inputLine : csvEx.getPossibleInputLines()) {
                assertLine("basic", fnBasic, inputLine, csvEx.getValues());
            }
        }
        
        BytesWritable bsw = new BytesWritable();

        // Empty lines are skipped
        assertThat(fnBasic.processBundle(bsw), hasSize(0));

        // TODO: Error when three quotes
        // assertLine("basic", fnBasic, "\"\"\"", "\"");
    }

}
