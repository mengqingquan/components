package org.talend.components.simplefileio.runtime.hadoop.excel;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.talend.components.simplefileio.runtime.hadoop.excel.streaming.StreamingReader;
import org.talend.daikon.avro.NameUtil;

/**
 * https://github.com/apache/poi/tree/trunk/src/examples/src/org/apache/poi/xssf
 * /usermodel/examples
 * 
 * @author wangwei
 *
 */
public class ExcelFileRecordReader extends RecordReader<Void, IndexedRecord> {
  private static final Log LOG = LogFactory.getLog(ExcelFileRecordReader.class);

  private Workbook workbook;
  
  private Workbook stream_workbook;
  
  private Sheet sheet;

  private IndexedRecord value;

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
  
  //use stream api for excel 2007 for support huge excel
  private boolean isExcel2007;

  public ExcelFileRecordReader() {
  }

  public ExcelFileRecordReader(String encoding, String sheet, long header, long footer, String excelFormat) throws UnsupportedEncodingException {
    this.encoding = encoding;
    this.sheetName = sheet;
    this.header = header;
    this.footer = footer;
    this.isHtml = "HTML".equals(excelFormat);
    
    isExcel2007 = "EXCEL2007".equals(excelFormat);
  }

  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();

    final Path file = split.getPath();

    InputStream in = createInputStream(job, file);
    
    if(isHtml) {
      init4ExcelHtml(in);
      return;
    } else if(isExcel2007) {
      init4Excel2007(in, job, file);
      return;
    }
    
    init4Excel97(in);
  }

  private InputStream createInputStream(Configuration job, final Path file) throws IOException {
    final FileSystem fs = file.getFileSystem(job);
    InputStream in = fs.open(file);

    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
    if (null != codec) {
      decompressor = CodecPool.getDecompressor(codec);
      in = codec.createInputStream(in, decompressor);
    }
    return in;
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
    boolean isSchemaHeader = header < 2;
    
    htmlRowIterator = rows.iterator();
    
    //we use it to fetch the schema
    List<String> headerRow = null;
    
    while ((header--) > 0 && htmlRowIterator.hasNext()) {
      currentRow++;
      headerRow = htmlRowIterator.next();
    }
    
    //as only one task to process the excel as no split, so we can do that like this
    if(isSchemaHeader && headerRow!=null && !headerRow.isEmpty()) {
      schema = createSchema(headerRow, true);
    }
  }
  
  private Schema createSchema(List<String> headerRow, boolean validName) {
    SchemaBuilder.FieldAssembler<Schema> fa = SchemaBuilder.record(FIELD_PREFIX).fields();
    
    if(headerRow!=null) {
      Set<String> existNames = new HashSet<String>();
      int index = 0;
      
      for (int i = 0; i < headerRow.size(); i++) {
          String fieldName = validName ? headerRow.get(i) : (FIELD_PREFIX + i);
          
          String finalName = NameUtil.correct(fieldName, index++, existNames);
          existNames.add(finalName);
          
          fa = fa.name(finalName).type(Schema.create(Schema.Type.STRING)).noDefault();
      }
    }
    
    return fa.endRecord();
  }
  
  private void init4Excel97(InputStream in) throws IOException {
    try {
      workbook = WorkbookFactory.create(in);
    } catch (EncryptedDocumentException | InvalidFormatException e) {
      throw new RuntimeException("failed to create workbook object : " + e.getMessage());
    }

    if(workbook.getNumberOfSheets() > 0) {
      sheet = workbook.getSheetAt(0);
      for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
        String sheetName = workbook.getSheetName(i);
        if (sheetName.equals(this.sheetName)) {
          sheet = workbook.getSheetAt(i);
          break;
        }
      }
    }

    if (sheet == null) {
      throw new RuntimeException("can't find the sheet : " + sheetName);
    }
    
    formulaEvaluator = workbook.getCreationHelper().createFormulaEvaluator();

    endRow = sheet.getLastRowNum() + 1 - footer;

    // skip header
    rowIterator = sheet.iterator();
    
    //we use it to fetch the schema
    Row headerRow = null;
    
    while ((header--) > 0 && rowIterator.hasNext()) {
      currentRow++;
      headerRow = rowIterator.next();
    }
    
    //as only one task to process the excel as no split, so we can do that like this
    if(!ExcelUtils.isEmptyRow(headerRow)) {
      schema = createSchema(headerRow, false);
    }
  }
  
  private void init4Excel2007(InputStream in, Configuration job, Path file) throws IOException {
    Sheet sheet = initStreamWorkbookAndSheet(in);

    endRow = Long.MAX_VALUE;
    if(footer > 0) {
      try {
        //read the whole file one time by stream for get the end row
        long rowNum = 0;
        for(Row row : sheet) {
          rowNum++;
        }
        
        endRow = rowNum - footer;
      } finally {
        if(stream_workbook != null) {
          stream_workbook.close();
        }
      }
      
      //recreate the stream sheet for the second time to read
      sheet = initStreamWorkbookAndSheet(this.createInputStream(job, file));
    }

    // skip header
    rowIterator = sheet.iterator();
    
    //we use it to fetch the schema
    Row headerRow = null;
    
    while ((header--) > 0 && rowIterator.hasNext()) {
      currentRow++;
      headerRow = rowIterator.next();
    }
    
    //as only one task to process the excel as no split, so we can do that like this
    if(!ExcelUtils.isEmptyRow4Stream(headerRow)) {
      schema = createSchema(headerRow, false);
    }
  }

  private Sheet initStreamWorkbookAndSheet(InputStream in) {
    stream_workbook = StreamingReader.builder()
        .bufferSize(4096)
        .rowCacheSize(1)
        .open(in);

    Sheet sheet = StringUtils.isEmpty(this.sheetName) ?
        stream_workbook.getSheetAt(0) : stream_workbook.getSheet(this.sheetName);
        
    if (sheet == null) {
      throw new RuntimeException("can't find the sheet : " + sheetName);
    }
    
    return sheet;
  }
  
  private static final String RECORD_NAME = "StringArrayRecord";

  private static final String FIELD_PREFIX = "field";
  
  private Schema createSchema(Row headerRow, boolean validName) {
    SchemaBuilder.FieldAssembler<Schema> fa = SchemaBuilder.record(RECORD_NAME).fields();
    
    if(headerRow!=null) {
      Set<String> existNames = new HashSet<String>();
      int index = 0;
      
      int i = 0;
      for (Cell cell : headerRow) {
          String fieldName = validName ? (isExcel2007 ? (cell == null ? StringUtils.EMPTY : cell.getStringCellValue()) : ExcelUtils.getCellValueAsString(cell, formulaEvaluator)) : (FIELD_PREFIX + (i++));
          
          String finalName = NameUtil.correct(fieldName, index++, existNames);
          existNames.add(finalName);
          
          fa = fa.name(finalName).type(Schema.create(Schema.Type.STRING)).noDefault();
      }
    }
    
    return fa.endRecord();
  }

  private Schema schema;
  
  public boolean nextKeyValue() throws IOException {
    if (currentRow >= endRow) {
      return false;
    }
    
    if(isHtml) {
      return nextKeyValue4ExcelHtml();
    } else if(isExcel2007) {
      return nextKeyValue4Excel2007();
    }
    return nextKeyValue4Excel97();
  }
  
  private boolean nextKeyValue4ExcelHtml() {
    if (!htmlRowIterator.hasNext()) {
      return false;
    }

    currentRow++;

    List<String> row = htmlRowIterator.next();

    //if not fill the schema before as no header or invalid header, set it here and as no valid name as no header, so set a name like this : field1,field2,field3
    if(schema == null) {
      schema = createSchema(row, false);
    }
    value = new GenericData.Record(schema);
   
    List<Field> fields = schema.getFields();
    
    for (int i=0;i<row.size();i++) {
      if(i<fields.size()) {
        value.put(i, row.get(i));
      }
    }
    
    return true;
  }

  private boolean nextKeyValue4Excel2007() throws IOException {
    if (!rowIterator.hasNext()) {
      return false;
    }

    currentRow++;

    Row row = rowIterator.next();
    
    if(ExcelUtils.isEmptyRow4Stream(row)) {
      //skip empty rows
      return nextKeyValue();
    }

    //if not fill the schema before as no header or invalid header, set it here and as no valid name as no header, so set a name like this : field1,field2,field3
    if(schema == null) {
      schema = createSchema(row, false);
    }
    value = new GenericData.Record(schema);
    
    List<Field> fields = schema.getFields();
    
    int i = 0;
    for (Cell cell : row) {
      if(i < fields.size()) {
        String content = cell == null ? StringUtils.EMPTY : cell.getStringCellValue();
        value.put(i++, content);
      }
    }

    return true;
  }
  
  private boolean nextKeyValue4Excel97() throws IOException {
    if (!rowIterator.hasNext()) {
      return false;
    }

    currentRow++;

    Row row = rowIterator.next();
    
    if(ExcelUtils.isEmptyRow(row)) {
      //skip empty rows
      return nextKeyValue();
    }

    //if not fill the schema before as no header or invalid header, set it here and as no valid name as no header, so set a name like this : field1,field2,field3
    if(schema == null) {
      schema = createSchema(row, false);
    }
    value = new GenericData.Record(schema);
    
    List<Field> fields = schema.getFields();
    
    int i = 0;
    for (Cell cell : row) {
      if(i < fields.size()) {
        String content = ExcelUtils.getCellValueAsString(cell, formulaEvaluator);
        value.put(i++, content);
      }
    }

    return true;
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public IndexedRecord getCurrentValue() {
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
      
      if(stream_workbook != null) {
        stream_workbook.close();
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }
  }
}