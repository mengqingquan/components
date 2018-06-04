package org.talend.components.simplefileio.runtime.hadoop.excel;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;

public class ExcelFileRecordReader extends RecordReader<Void, TextArrayWriteable> {
  private static final Log LOG = LogFactory.getLog(ExcelFileRecordReader.class);

  private long start;
  private long end;
  
  private SplitLineReader in;
  
  private FSDataInputStream fileIn;
  private LongWritable key;

  private Text value;
  private TextArrayWriteable rowValue;

  private Decompressor decompressor;

  private String encoding = "UTF-8";

  public ExcelFileRecordReader() {
  }

  public ExcelFileRecordReader(String encoding) throws UnsupportedEncodingException {
      this.encoding = encoding;
  }

  //TODO
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    
    start = split.getStart();
    end = start + split.getLength();
    
    final Path file = split.getPath();

    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);

    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
    if (null != codec) {
      //TODO
      CompressionInputStream cis = codec.createInputStream(fileIn, decompressor);
      in = null;
    } else {
      in = null;
    }
  }

  //TODO
  public boolean nextKeyValue() throws IOException {
      return false;
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public TextArrayWriteable getCurrentValue() {
    return rowValue;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      //TODO fix the progress, how to do for excel?
      return 0.0f;
    }
  }

  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }
  }
}