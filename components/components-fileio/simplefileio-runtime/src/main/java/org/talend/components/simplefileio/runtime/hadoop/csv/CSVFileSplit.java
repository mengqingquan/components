package org.talend.components.simplefileio.runtime.hadoop.csv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CSVFileSplit extends FileSplit {

  private long skipLength = 0l;

  public CSVFileSplit() {
  }

  public CSVFileSplit(Path file, long start, long length, long skipLineLength, String[] hosts) {
    super(file, start, length, hosts);
    this.skipLength = skipLineLength;
  }

  public CSVFileSplit(Path file, long start, long length, long skipLineLength, String[] hosts, String[] inMemoryHosts) {
    super(file, start, length, hosts, inMemoryHosts);
    this.skipLength = skipLineLength;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(skipLength);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    skipLength = in.readLong();
  }

  public long getSkipLineLength() {
    return skipLength;
  }
}
