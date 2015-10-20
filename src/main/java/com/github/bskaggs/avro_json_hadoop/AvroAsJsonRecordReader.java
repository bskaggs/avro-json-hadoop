package com.github.bskaggs.avro_json_hadoop;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.io.TerseJsonEncoder;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract base class for <code>RecordReader</code>s that read Avro container files.
 *
 * @param <K> The type of key the record reader should generate.
 * @param <V> The type of value the record reader should generate.
 * @param <T> The type of the entries within the Avro container file being read.
 */
public class AvroAsJsonRecordReader extends RecordReader<Text, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroAsJsonRecordReader.class);

  /** The current record from the Avro container file being read. */
  private Text text = new Text();
  private ByteArrayOutputStream bout = new ByteArrayOutputStream();

  /** A reader for the Avro container file containing the current input split. */
  private DataFileReader<Object> mAvroFileReader;

  /**
   * The byte offset into the Avro container file where the first block that fits
   * completely within the current input split begins.
   */
  private long mStartPosition;

  /** The byte offset into the Avro container file where the current input split ends. */
  private long mEndPosition;

	private DatumWriter<Object> writer;
	
	private JsonEncoder encoder;

  /**
   * Constructor.
   */
  protected AvroAsJsonRecordReader() {  }

  /** {@inheritDoc} */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    if (!(inputSplit instanceof FileSplit)) {
      throw new IllegalArgumentException("Only compatible with FileSplits.");
    }
    FileSplit fileSplit = (FileSplit) inputSplit;

    // Open a seekable input stream to the Avro container file.
    SeekableInput seekableFileInput = createSeekableInput(context.getConfiguration(), fileSplit.getPath());

    // Wrap the seekable input stream in an Avro DataFileReader.
    Configuration conf = context.getConfiguration();
    GenericData dataModel = AvroSerialization.createDataModel(conf);
    
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
    
    //Figure out the schema
    Path path = fileSplit.getPath();
    FSDataInputStream schemaStream = path.getFileSystem(conf).open(path);
    DataFileStream<Object> streamReader = new DataFileStream<Object>(schemaStream, reader);
    Schema mReaderSchema = streamReader.getSchema();
    streamReader.close();
    
    //Set up writer and encoder for json
    writer = new GenericDatumWriter<Object>(mReaderSchema);
    encoder = new TerseJsonEncoder(mReaderSchema, bout);
    
    @SuppressWarnings("unchecked")
	DatumReader<Object> datumReader = dataModel.createDatumReader(mReaderSchema);
    mAvroFileReader = createAvroFileReader(seekableFileInput, datumReader);

    // Initialize the start and end offsets into the file based on the boundaries of the
    // input split we're responsible for.  We will read the first block that begins
    // after the input split start boundary.  We will read up to but not including the
    // first block that starts after input split end boundary.

    // Sync to the closest block/record boundary just after beginning of our input split.
    mAvroFileReader.sync(fileSplit.getStart());

    // Initialize the start position to the beginning of the first block of the input split.
    mStartPosition = mAvroFileReader.previousSync();

    // Initialize the end position to the end of the input split (this isn't necessarily
    // on a block boundary so using this for reporting progress will be approximate.
    mEndPosition = fileSplit.getStart() + fileSplit.getLength();
  }

  /** {@inheritDoc} */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    assert null != mAvroFileReader;

    if (mAvroFileReader.hasNext() && !mAvroFileReader.pastSync(mEndPosition)) {
      bout.reset();
      writer.write(mAvroFileReader.next(null), encoder);
      encoder.flush();
      text.set(bout.toByteArray());
      return true;
    }
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    assert null != mAvroFileReader;

    if (mEndPosition == mStartPosition) {
      // Trivial empty input split.
      return 0.0f;
    }
    long bytesRead = mAvroFileReader.previousSync() - mStartPosition;
    long bytesTotal = mEndPosition - mStartPosition;
    LOG.debug("Progress: bytesRead=" + bytesRead + ", bytesTotal=" + bytesTotal);
    return Math.min(1.0f, (float) bytesRead / (float) bytesTotal);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    if (null != mAvroFileReader) {
      try {
        mAvroFileReader.close();
      } finally {
        mAvroFileReader = null;
      }
    }
  }

  /**
   * Gets the current record read from the Avro container file.
   *
   * <p>Calling <code>nextKeyValue()</code> moves this to the next record.</p>
   *
   * @return The current Avro record (may be null if no record has been read).
   */
  protected Text getCurrentRecord() {
    return text;
  }

  /**
   * Creates a seekable input stream to an Avro container file.
   *
   * @param conf The hadoop configuration.
   * @param path The path to the avro container file.
   * @throws IOException If there is an error reading from the path.
   */
  protected SeekableInput createSeekableInput(Configuration conf, Path path)
      throws IOException {
    return new FsInput(path, conf);
  }

  /**
   * Creates an Avro container file reader from a seekable input stream.
   *
   * @param input The input containing the Avro container file.
   * @param datumReader The reader to use for the individual records in the Avro container file.
   * @throws IOException If there is an error reading from the input stream.
   */
  protected DataFileReader<Object> createAvroFileReader(
      SeekableInput input, DatumReader<Object> datumReader) throws IOException {
    return new DataFileReader<Object>(input, datumReader);
  }

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return text;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}
}