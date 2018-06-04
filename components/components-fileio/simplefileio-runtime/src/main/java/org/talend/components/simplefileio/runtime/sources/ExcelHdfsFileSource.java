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
package org.talend.components.simplefileio.runtime.sources;

import java.io.IOException;

import org.talend.components.simplefileio.runtime.ExtraHadoopConfiguration;
import org.talend.components.simplefileio.runtime.SimpleFileIOAvroRegistry;
import org.talend.components.simplefileio.runtime.hadoop.excel.ExcelFileInputFormat;
import org.talend.components.simplefileio.runtime.hadoop.excel.TextArrayWriteable;
import org.talend.components.simplefileio.runtime.ugi.UgiDoAs;

/**
 * Excel implementation of HDFSFileSource.
 *
 */
public class ExcelHdfsFileSource extends FileSourceBase<Void, TextArrayWriteable, ExcelHdfsFileSource> {

    static {
        // Ensure that the singleton for the SimpleFileIOAvroRegistry is created.
        SimpleFileIOAvroRegistry.get();
    }

    private ExcelHdfsFileSource(UgiDoAs doAs, String filepattern, String encoding, ExtraHadoopConfiguration extraConfig,
            SerializableSplit serializableSplit) {
        super(doAs, filepattern, ExcelFileInputFormat.class, Void.class, TextArrayWriteable.class, extraConfig, serializableSplit);
        ExtraHadoopConfiguration hadoop_config = getExtraHadoopConfiguration();
        hadoop_config.set(ExcelFileInputFormat.TALEND_ENCODING, encoding);
    }

    private ExcelHdfsFileSource(UgiDoAs doAs, String filepattern, ExtraHadoopConfiguration extraConfig,
            SerializableSplit serializableSplit) {
        super(doAs, filepattern, ExcelFileInputFormat.class, Void.class, TextArrayWriteable.class, extraConfig, serializableSplit);
    }

    public static ExcelHdfsFileSource of(UgiDoAs doAs, String filepattern, String encoding) {
        return new ExcelHdfsFileSource(doAs, filepattern, encoding, new ExtraHadoopConfiguration(), null);
    }

    @Override
    protected ExcelHdfsFileSource createSourceForSplit(SerializableSplit serializableSplit) {
        ExcelHdfsFileSource source = new ExcelHdfsFileSource(doAs, filepattern, getExtraHadoopConfiguration(), serializableSplit);
        source.setLimit(getLimit());
        return source;
    }

    @Override
    protected UgiFileReader createReaderForSplit(SerializableSplit serializableSplit) throws IOException {
        return new UgiFileReader(this);
    }
}
