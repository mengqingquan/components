package org.talend.components.simplefileio.runtime.hadoop.csv;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public abstract class TDelimitedFileRecordReader<K, V> implements RecordReader<K, V> {
    public static final String MAX_LINE_LENGTH =
          "mapreduce.input.linerecordreader.line.maxlength";
    private CompressionCodecFactory compressionCodecs = null;

    private long start;

    private long pos;

    private long end;

    private TDelimitedFileLineReader in;

    int maxLineLength;

    protected TDelimitedFileRecordReader(JobConf job, FileSplit split, byte[] rowSeparator) throws IOException {
        this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(file);
        if (codec != null) {
            InputStream inputStream = codec.createInputStream(fileIn);
            inputStream.skip(start);
            in = new TDelimitedFileLineReader(inputStream, TDelimitedFileLineReader.DEFAULT_BUFFER_SIZE,
                    rowSeparator, split.getLength());
            end = Long.MAX_VALUE;
        } else {
            fileIn.seek(start);
            in = new TDelimitedFileLineReader(fileIn, TDelimitedFileLineReader.DEFAULT_BUFFER_SIZE, rowSeparator,
                    split.getLength());
        }
        // Support the header
        long skipline = 0;
        if (split instanceof TFileSplit) {
            skipline = ((TFileSplit)split).getSkipLineLength();
        }
        if (start != skipline) {
            start += in.readLine(new Text(), 0, maxBytesToConsume(start));
        }
        this.pos = start;
    }

  private int maxBytesToConsume(long pos) {
        return (int) Math.max(Math.min(Integer.MAX_VALUE, end - pos), maxLineLength);
  }

  private int skipUtfByteOrderMark(Text value) throws IOException {
        // Strip BOM(Byte Order Mark)
        // Text only support UTF-8, we only need to check UTF-8 BOM
        // (0xEF,0xBB,0xBF) at the start of the text stream.
        int newMaxLineLength = (int) Math.min(3L + (long) maxLineLength, Integer.MAX_VALUE);
        int newSize = in.readLine(value, newMaxLineLength, maxBytesToConsume(pos));
        // Even we read 3 extra bytes for the first line,
        // we won't alter existing behavior (no backwards incompat issue).
        // Because the newSize is less than maxLineLength and
        // the number of bytes copied to Text is always no more than newSize.
        // If the return size from readLine is not less than maxLineLength,
        // we will discard the current line and read the next line.
        pos += newSize;
        int textLength = value.getLength();
        byte[] textBytes = value.getBytes();
        if ((textLength >= 3) && (textBytes[0] == (byte) 0xEF) && (textBytes[1] == (byte) 0xBB)
                && (textBytes[2] == (byte) 0xBF)) {
            // find UTF-8 BOM, strip it.
            textLength -= 3;
            newSize -= 3;
            if (textLength > 0) {
                // It may work to use the same buffer and not do the copyBytes
                textBytes = new byte[value.getLength()];
                System.arraycopy(value.getBytes(), 0, textBytes, 0, value.getLength());
                value.set(textBytes, 3, textLength);
            } else {
                value.clear();
            }
        }
        return newSize;
  }

    public boolean next(Text value) throws IOException {
        while (pos <= end || in.needAdditionalRecordAfterSplit()) {
            int newSize = 0;
            if (pos == 0) {
                newSize = skipUtfByteOrderMark(value);
            } else {
                newSize = in.readLine(value, maxLineLength, maxBytesToConsume(pos));
                pos += newSize;
            }
            if (newSize == 0) {
                return false;
            }
            if (newSize < maxLineLength) {
                return true;
            }
        }
        return false;
    }

    public long getPos() throws IOException {
        return pos;
    }

    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    public float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }
}

