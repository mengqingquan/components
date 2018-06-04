package org.talend.components.simplefileio.runtime.hadoop.excel;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.poi.ss.formula.eval.NumberEval;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * https://github.com/apache/poi/tree/trunk/src/examples/src/org/apache/poi/xssf
 * /usermodel/examples
 * 
 * @author wangwei
 *
 */
public class ExcelFileRecordReader extends RecordReader<Void, TextArrayWriteable> {
  private static final Log LOG = LogFactory.getLog(ExcelFileRecordReader.class);

  private XSSFWorkbook workbook;
  private Sheet sheet;

  private TextArrayWriteable value;

  private Decompressor decompressor;

  // TODO maybe remove the encoding for excel 2007 format as not used now
  private String encoding = "UTF-8";

  private String sheetName;
  private long header;
  private long footer;

  private long currentRow;
  private long endRow;

  private static DecimalFormat df = new DecimalFormat("#.####################################");

  private Iterator<Row> rowIterator;

  public ExcelFileRecordReader() {
  }

  public ExcelFileRecordReader(String encoding, String sheet, long header, long footer) throws UnsupportedEncodingException {
    this.encoding = encoding;
    this.sheetName = sheet;
    this.header = header;
    this.footer = footer;
  }

  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();

    final Path file = split.getPath();

    final FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(file);

    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
    if (null != codec) {
      CompressionInputStream cis = codec.createInputStream(fileIn, decompressor);
      workbook = new XSSFWorkbook(cis);
    } else {
      workbook = new XSSFWorkbook(fileIn);
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

    if (!rowIterator.hasNext()) {
      return false;
    }

    currentRow++;

    Row row = rowIterator.next();

    List<Text> list = new ArrayList<Text>();

    for (Cell cell : row) {
      String content = "";// TODO check if can set null
      CellType type = cell.getCellTypeEnum();
      switch (type) {
      case STRING:
        content = cell.getRichStringCellValue().getString();
        break;
      case NUMERIC:
        if (DateUtil.isCellDateFormatted(cell)) {
          content = cell.getDateCellValue().toString();
        } else {
          content = df.format(cell.getNumericCellValue());
        }
        break;
      case BOOLEAN:
        content = String.valueOf(cell.getBooleanCellValue());
        break;
      case FORMULA:
        CellType ct = cell.getCachedFormulaResultTypeEnum();
        switch (ct) {
        case STRING:
          content = cell.getRichStringCellValue().getString();
          break;
        case NUMERIC:
          if (DateUtil.isCellDateFormatted(cell)) {
            content = cell.getDateCellValue().toString();
          } else {
            content = new NumberEval(cell.getNumericCellValue()).getStringValue();
          }
          break;
        case BOOLEAN:
          content = String.valueOf(cell.getBooleanCellValue());
          break;
        default:
          break;
        }
        break;
      default:
        break;
      }

      Text text = new Text();
      text.set(content);
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