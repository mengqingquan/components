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

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.io.Writable;
import org.talend.components.simplefileio.ExcelFormat;
import org.talend.components.simplefileio.runtime.hadoop.excel.TextArrayWriteable;
import org.talend.components.simplefileio.runtime.sources.ExcelHdfsFileSource;
import org.talend.components.simplefileio.runtime.ugi.UgiDoAs;
import org.talend.daikon.avro.converter.IndexedRecordConverter;

public class SimpleRecordFormatExcelIO extends SimpleRecordFormatBase {

    static {
        // Ensure that the singleton for the SimpleFileIOAvroRegistry is created.
        SimpleFileIOAvroRegistry.get();
    }
    
    private String sheetName;
    private final String encoding;
    private final long header;
    private final long footer;
    private final ExcelFormat excelFormat;

    public SimpleRecordFormatExcelIO(UgiDoAs doAs, String path, boolean overwrite, int limit, boolean mergeOutput, String encoding, String sheetName, long header, long footer, ExcelFormat excelFormat) {
        super(doAs, path, overwrite, limit, mergeOutput);
        this.sheetName = sheetName;
        this.encoding = encoding;
        this.header = header;
        this.footer = footer;
        this.excelFormat = excelFormat;
    }

    @Override
    public PCollection<IndexedRecord> read(PBegin in) {
        ExcelHdfsFileSource source = ExcelHdfsFileSource.of(doAs, path, encoding, sheetName, header, footer, excelFormat.name());
        source.getExtraHadoopConfiguration().addFrom(getExtraHadoopConfiguration());
        source.setLimit(limit);
        PCollection<KV<Void, TextArrayWriteable>> pc1 = in.apply(Read.from(source));
        PCollection<TextArrayWriteable> pc2 = pc1.apply(Values.<TextArrayWriteable>create());
        PCollection<IndexedRecord> pc3 = pc2.apply(ParDo.of(new ExtractRecordFromRowObject()));
        return pc3;
    }

    public static class ExtractRecordFromRowObject extends DoFn<TextArrayWriteable, IndexedRecord> {

        static {
            // Ensure that the singleton for the SimpleFileIOAvroRegistry is created.
            SimpleFileIOAvroRegistry.get();
        }
        
        private transient IndexedRecordConverter<String[], ? extends IndexedRecord> converter;

        @ProcessElement
        public void processElement(ProcessContext c) {
            if (converter == null) {
                converter = new SimpleFileIOAvroRegistry.StringArrayToIndexedRecordConverter();
            }
            //TODO not good here
            TextArrayWriteable ta = c.element();
            Writable[] columns = (Writable[])ta.get();
            List<String> list = new ArrayList<String>();
            for(Writable column : columns) {
              list.add(column.toString());
            }
            String[] input = list.toArray(new String[0]);
            IndexedRecord output = converter.convertToAvro(input);
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
