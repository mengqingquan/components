package org.talend.components.simplefileio.runtime.hadoop.csv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Extends the jobSplit to add the Length variable
 *
 */
public class TFileSplit extends FileSplit {

  private long skipLineLength = 0l;

  public TFileSplit() {
    super(null, 0l, 0l, (String[])null);
  }
  
  public TFileSplit(Path file, long start, long length, long skipLineLength, String[] hosts) {
    super(file, start, length, hosts);
    this.skipLineLength = skipLineLength;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(skipLineLength);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    skipLineLength = in.readLong();
  }

  public long getSkipLineLength() {
    return skipLineLength;
  }
}
