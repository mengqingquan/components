package org.talend.components.simplefileio.runtime.hadoop.csv;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Text;

public class TDelimitedFileLineReader {

    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    private int bufferSize = DEFAULT_BUFFER_SIZE;

    private InputStream in;

    private byte[] buffer;

    // the number of bytes of real data in the buffer
    private int bufferLength = 0;

    // the current position in the buffer
    private int bufferPosn = 0;

    private boolean needAdditionalRecord = false;

    private long splitLength;

    /** Total bytes read from the input stream. */
    private long totalBytesRead = 0;

    private boolean finished = false;

    private boolean usingCRLF;

    // The line delimiter
    private final byte[] recordDelimiterBytes;

    public TDelimitedFileLineReader(InputStream in, int bufferSize, byte[] recordDelimiterBytes, long splitLength) {
        this.in = in;
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
        this.usingCRLF = (recordDelimiterBytes == null);
        this.splitLength = splitLength;
    }

    public void close() throws IOException {
        in.close();
    }

    protected int fillBuffer(InputStream in, byte[] buffer, boolean inDelimiter) throws IOException {
        if (in instanceof org.apache.hadoop.io.compress.DecompressorStream
                || in instanceof org.apache.hadoop.io.compress.CompressionInputStream) {
            int bytesRead = in.read(buffer);

            // If the split ended in the middle of a record delimiter then we need
            // to read one additional record, as the consumer of the next split will
            // not recognize the partial delimiter as a record.
            // However if using the default delimiter and the next character is a
            // linefeed then next split will treat it as a delimiter all by itself
            // and the additional record read should not be performed.
            if (inDelimiter && bytesRead > 0) {
                if (usingCRLF) {
                    needAdditionalRecord = (buffer[0] != '\n');
                } else {
                    needAdditionalRecord = true;
                }
                finished = !needAdditionalRecord;
            }
            return bytesRead;
        } else {
            int maxBytesToRead = buffer.length;
            if (totalBytesRead < splitLength) {
                long bytesLeftInSplit = splitLength - totalBytesRead;
                if (bytesLeftInSplit < maxBytesToRead) {
                    maxBytesToRead = (int) bytesLeftInSplit;
                }
            }
            int bytesRead = in.read(buffer, 0, maxBytesToRead);
            // If the split ended in the middle of a record delimiter then we need
            // to read one additional record, as the consumer of the next split will
            // not recognize the partial delimiter as a record.
            // However if using the default delimiter and the next character is a
            // linefeed then next split will treat it as a delimiter all by itself
            // and the additional record read should not be performed.
            if (totalBytesRead == splitLength && inDelimiter && bytesRead > 0) {
                if (usingCRLF) {
                    needAdditionalRecord = (buffer[0] != '\n');
                } else {
                    needAdditionalRecord = true;
                }
            }
            if (bytesRead > 0) {
                totalBytesRead += bytesRead;
            }
            return bytesRead;
        }
    }

    public int readLineSplitLineReader(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
        str.clear();
        int txtLength = 0; // tracks str.getLength(), as an optimization
        long bytesConsumed = 0;
        int delPosn = 0;
        int ambiguousByteCount = 0; // To capture the ambiguous characters count
        do {
            int startPosn = bufferPosn; // Start from previous end position
            if (bufferPosn >= bufferLength) {
                startPosn = bufferPosn = 0;
                bufferLength = fillBuffer(in, buffer, ambiguousByteCount > 0);
                if (bufferLength <= 0) {
                    if (ambiguousByteCount > 0) {
                        str.append(recordDelimiterBytes, 0, ambiguousByteCount);
                        bytesConsumed += ambiguousByteCount;
                    }
                    break; // EOF
                }
            }
            for (; bufferPosn < bufferLength; ++bufferPosn) {
                if (buffer[bufferPosn] == recordDelimiterBytes[delPosn]) {
                    delPosn++;
                    if (delPosn >= recordDelimiterBytes.length) {
                        bufferPosn++;
                        break;
                    }
                } else if (delPosn != 0) {
                    bufferPosn -= delPosn;
                    if (bufferPosn < -1) {
                        bufferPosn = -1;
                    }
                    delPosn = 0;
                }
            }
            int readLength = bufferPosn - startPosn;
            bytesConsumed += readLength;
            int appendLength = readLength - delPosn;
            if (appendLength > maxLineLength - txtLength) {
                appendLength = maxLineLength - txtLength;
            }
            bytesConsumed += ambiguousByteCount;
            if (appendLength >= 0 && ambiguousByteCount > 0) {
                // appending the ambiguous characters (refer case 2.2)
                str.append(recordDelimiterBytes, 0, ambiguousByteCount);
                ambiguousByteCount = 0;
                // since it is now certain that the split did not split a delimiter we
                // should not read the next record: clear the flag otherwise duplicate
                // records could be generated
                unsetNeedAdditionalRecordAfterSplit();
            }
            if (appendLength > 0) {
                str.append(buffer, startPosn, appendLength);
                txtLength += appendLength;
            }
            if (bufferPosn >= bufferLength) {
                if (delPosn > 0 && delPosn < recordDelimiterBytes.length) {
                    ambiguousByteCount = delPosn;
                    bytesConsumed -= ambiguousByteCount; // to be consumed in next
                }
            }
        } while (delPosn < recordDelimiterBytes.length && bytesConsumed < maxBytesToConsume);
        if (bytesConsumed > Integer.MAX_VALUE) {
            throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
        }
        return (int) bytesConsumed;
    }

    public int readLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
        int bytesRead = 0;
        if (!finished) {
            if (in instanceof org.apache.hadoop.io.compress.DecompressorStream
                    || in instanceof org.apache.hadoop.io.compress.CompressionInputStream) {
                // nothing
            } else {
                // only allow at most one more record to be read after the stream
                // reports the split ended
                if (totalBytesRead > splitLength) {
                    finished = true;
                }
            }
            bytesRead = readLineSplitLineReader(str, maxLineLength, maxBytesToConsume);
        }
        return bytesRead;
    }

    public int readLine(Text str, int maxLineLength) throws IOException {
        return readLine(str, maxLineLength, Integer.MAX_VALUE);
    }

    public int readLine(Text str) throws IOException {
        return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public boolean needAdditionalRecordAfterSplit() {
        return !finished && needAdditionalRecord;
    }

    protected void unsetNeedAdditionalRecordAfterSplit() {
        needAdditionalRecord = false;
    }
}

