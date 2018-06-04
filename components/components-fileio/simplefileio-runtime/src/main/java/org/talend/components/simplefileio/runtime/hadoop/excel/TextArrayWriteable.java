package org.talend.components.simplefileio.runtime.hadoop.excel;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class TextArrayWriteable extends ArrayWritable {
  public TextArrayWriteable() {
    super(Text.class);
  }
}
