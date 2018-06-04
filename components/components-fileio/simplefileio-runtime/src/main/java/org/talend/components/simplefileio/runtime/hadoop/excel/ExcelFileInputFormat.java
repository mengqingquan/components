package org.talend.components.simplefileio.runtime.hadoop.excel;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ExcelFileInputFormat extends org.apache.hadoop.mapreduce.lib.input.FileInputFormat<Void, TextArrayWriteable> {

  public static String TALEND_ENCODING = "talend_excel_encoding";
  
  public static String TALEND_HEADER = "talend_excel_header";

  private static final Log LOG = LogFactory.getLog(ExcelFileInputFormat.class);

  @Override
  public ExcelFileRecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
    String encoding = context.getConfiguration().get(TALEND_ENCODING);
    return new ExcelFileRecordReader(encoding);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
      return false;
  }

}
