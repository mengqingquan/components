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
package org.talend.components.simplefileio.runtime;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.talend.components.simplefileio.runtime.hadoop.excel.TextArrayWriteable;
import org.talend.components.simplefileio.runtime.sources.ExcelHdfsFileSource;
import org.talend.components.simplefileio.runtime.ugi.UgiDoAs;

public class SimpleRecordFormatExcelIO extends SimpleRecordFormatBase {

    static {
        // Ensure that the singleton for the SimpleFileIOAvroRegistry is created.
        //TODO support excel schema support
        SimpleFileIOAvroRegistry.get();
    }
    
    private final String encoding;

    public SimpleRecordFormatExcelIO(UgiDoAs doAs, String path, boolean overwrite, int limit, boolean mergeOutput, String encoding) {
        super(doAs, path, overwrite, limit, mergeOutput);
        this.encoding = encoding;
    }

    @Override
    public PCollection<IndexedRecord> read(PBegin in) {
        ExcelHdfsFileSource source = ExcelHdfsFileSource.of(doAs, path, encoding);
        source.getExtraHadoopConfiguration().addFrom(getExtraHadoopConfiguration());
        source.setLimit(limit);
        PCollection<KV<Void, TextArrayWriteable>> pc1 = in.apply(Read.from(source));
        PCollection<IndexedRecord> pc2 = pc1.apply(ParDo.of(new ExtractRecordFromRowObject()));
        return pc2;
    }

    public static class ExtractRecordFromRowObject extends DoFn<KV<Void, TextArrayWriteable>, IndexedRecord> {

        static {
            // Ensure that the singleton for the SimpleFileIOAvroRegistry is created.
            SimpleFileIOAvroRegistry.get();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            TextArrayWriteable ta = c.element().getValue();
            //TODO
            IndexedRecord output = null;
            c.output(output);
        }
    }

    //now no output support, so TODO
    @Override
    public PDone write(PCollection<IndexedRecord> in) {
      // TODO Auto-generated method stub
      return null;
    }

}
