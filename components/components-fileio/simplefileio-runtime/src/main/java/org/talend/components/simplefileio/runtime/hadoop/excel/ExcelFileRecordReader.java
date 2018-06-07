package org.talend.components.simplefileio.runtime.hadoop.excel;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

/**
 * https://github.com/apache/poi/tree/trunk/src/examples/src/org/apache/poi/xssf
 * /usermodel/examples
 * 
 * @author wangwei
 *
 */
public class ExcelFileRecordReader extends RecordReader<Void, TextArrayWriteable> {
  private static final Log LOG = LogFactory.getLog(ExcelFileRecordReader.class);

  private Workbook workbook;
  
  private Sheet sheet;

  private TextArrayWriteable value;

  private Decompressor decompressor;

  // TODO maybe remove the encoding for excel 2007 and 97 format as not used in the poi api
  private String encoding = "UTF-8";

  private String sheetName;
  private long header;
  private long footer;

  private long currentRow;
  private long endRow;
  
  private FormulaEvaluator formulaEvaluator;

  private Iterator<Row> rowIterator;
  
  private boolean isHtml;
  private List<List<String>> rows;
  private Iterator<List<String>> htmlRowIterator;

  public ExcelFileRecordReader() {
  }

  public ExcelFileRecordReader(String encoding, String sheet, long header, long footer, String excelFormat) throws UnsupportedEncodingException {
    this.encoding = encoding;
    this.sheetName = sheet;
    this.header = header;
    this.footer = footer;
    this.isHtml = "HTML".equals(excelFormat);
  }

  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();

    final Path file = split.getPath();

    final FileSystem fs = file.getFileSystem(job);
    InputStream in = fs.open(file);

    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
    if (null != codec) {
      in = codec.createInputStream(in, decompressor);
    }
    
    if(isHtml) {
      init4ExcelHtml(in);
      return;
    }
    
    init4Excel2007And97(in);
  }

  private void init4ExcelHtml(InputStream in) {
    try {
      rows = ExcelHtmlParser.getRows(in, this.encoding);
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        LOG.warn("Failed to close the stream : " + e);
      }
    }
    
    endRow = rows.size() - footer;
    //for html format, the first line is always the schema show, we don't read it always now, so header 1 or 0 both mean skip the schema row only.
    //TODO now we implement the header like "skip lines" which is clear name for the implement, need to consider what the header work for? for schema retrieve? or for skip lines only?
    header = Math.max(1, header);
    
    htmlRowIterator = rows.iterator();
    while ((header--) > 0 && htmlRowIterator.hasNext()) {
      currentRow++;
      htmlRowIterator.next();
    }
  }

  private void init4Excel2007And97(InputStream in) throws IOException {
    try {
      workbook = WorkbookFactory.create(in);
    } catch (EncryptedDocumentException | InvalidFormatException e) {
      throw new RuntimeException("failed to create workbook object : " + e.getMessage());
    }

    for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
      String sheetName = workbook.getSheetName(i);
      if (sheetName.equals(this.sheetName)) {
        sheet = workbook.getSheetAt(i);
      }
    }

    if (sheet == null) {
      throw new RuntimeException("can't find the sheet : " + sheetName);
    }
    
    formulaEvaluator = workbook.getCreationHelper().createFormulaEvaluator();

    endRow = sheet.getLastRowNum() + 1 - footer;

    // skip header
    rowIterator = sheet.iterator();
    while ((header--) > 0 && rowIterator.hasNext()) {
      currentRow++;
      rowIterator.next();
    }
  }

  public boolean nextKeyValue() throws IOException {
    if (value == null) {
      value = new TextArrayWriteable();
    }
    
    if (currentRow >= endRow) {
      return false;
    }
    
    if(isHtml) {
      return nextKeyValue4ExcelHtml();
    }
    return nextKeyValue4Excel2007And97();
  }
  
  private boolean nextKeyValue4ExcelHtml() {
    if (!htmlRowIterator.hasNext()) {
      return false;
    }

    currentRow++;

    List<String> row = htmlRowIterator.next();

    List<Text> list = new ArrayList<Text>();

    for (String column : row) {
      Text text = new Text(column);
      list.add(text);
    }

    Text[] contents = list.toArray(new Text[0]);
    value.set(contents);
    return true;
  }

  private boolean nextKeyValue4Excel2007And97() {
    if (!rowIterator.hasNext()) {
      return false;
    }

    currentRow++;

    Row row = rowIterator.next();

    List<Text> list = new ArrayList<Text>();

    for (Cell cell : row) {
      String content = ExcelUtils.getCellValueAsString(cell, formulaEvaluator);
      Text text = new Text(content);
      list.add(text);
    }

    Text[] contents = list.toArray(new Text[0]);
    value.set(contents);
    return true;
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public TextArrayWriteable getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split, TODO not right in fact, the most time
   * for this is parsing excel file to object, not the reading object part
   */
  public float getProgress() throws IOException {
    return currentRow / (endRow - header);
  }

  public synchronized void close() throws IOException {
    try {
      if (workbook != null) {
        workbook.close();
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }
  }
}