package org.apache.pig.impl.io;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.Tuple;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.LineReader;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileInputFormat.FileBaseStatistics;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;

/**
 * 
 * Copy of DelimitedInputFormat with the only difference that the methods
 * readRecord() and nextRecord() have Tuple as targets instead of PactRecord
 * @author hinata
 *
 */
public abstract class SPigDelimitedInputFormat extends SPigFileInputFormat {
	
	/**
	 * The log.
	 */
	private static final Log LOG = LogFactory.getLog(SPigDelimitedInputFormat.class);
	
	/**
	 * The default read buffer size = 1MB.
	 */
	private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;
	
	/**
	 * The default number of sample lines to consider when calculating the line width.
	 */
	private static final int DEFAULT_NUM_SAMPLES = 10;
	
	// ------------------------------------- Config Keys ------------------------------------------
	
	/**
	 * The configuration key to set the record delimiter.
	 */
	public static final String RECORD_DELIMITER = "delimited-format.delimiter";
	
	/**
	 * The configuration key to set the record delimiter encoding.
	 */
	private static final String RECORD_DELIMITER_ENCODING = "delimited-format.delimiter-encoding";
	
	/**
	 * The configuration key to set the number of samples to take for the statistics.
	 */
	private static final String NUM_STATISTICS_SAMPLES = "delimited-format.numSamples";
	
	// --------------------------------------------------------------------------------------------
	
	protected byte[] readBuffer;

	protected byte[] wrapBuffer;

	protected int readPos;

	protected int limit;

	protected byte[] delimiter = new byte[] {'\n'};
	
	private byte[] currBuffer;
	private int currOffset;
	private int currLen;

	protected boolean overLimit;

	protected boolean end;
	
	protected int bufferSize = -1;
	
	protected int numLineSamples;										// the number of lines to sample for statistics
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * This function parses the given byte array which represents a serialized key/value
	 * pair. The parsed content is then returned by setting the pair variables. If the
	 * byte array contains invalid content the record can be skipped by returning <tt>false</tt>.
	 * 
	 * @param record The holder for the line that is read.
	 * @param bytes The serialized record.
	 * @return returns whether the record was successfully deserialized
	 */
	public abstract boolean readRecord(Tuple target, byte[] bytes, int offset, int numBytes);

	// --------------------------------------------------------------------------------------------
	/**
	 * Gets the delimiter that defines the record boundaries.
	 * 
	 * @return The delimiter, as bytes.
	 */
	public byte[] getDelimiter()
	{
		return this.delimiter;
	}
	
	/**
	 * Sets the size of the buffer to be used to find record boundaries. This method has only an effect, if it is called
	 * before the input format is opened.
	 * 
	 * @param bufferSize The buffer size to use.
	 */
	public void setBufferSize(int bufferSize)
	{
		this.bufferSize = bufferSize;
	}
	
	/**
	 * Gets the size of the buffer internally used to parse record boundaries.
	 * 
	 * @return The size of the parsing buffer.
	 */
	public int getBufferSize()
	{
		return this.readBuffer == null ? 0: this.readBuffer.length;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Configures this input format by reading the path to the file from the configuration and the string that
	 * defines the record delimiter.
	 * 
	 * @param parameters The configuration object to read the parameters from.
	 */
	@Override
	public void configure(Configuration parameters)
	{
		super.configure(parameters);
		
		final String delimString = parameters.getString(RECORD_DELIMITER, AbstractConfigBuilder.NEWLINE_DELIMITER);
		if (delimString == null) {
			throw new IllegalArgumentException("The delimiter not be null.");
		}
		final String charsetName = parameters.getString(RECORD_DELIMITER_ENCODING, null);

		try {
			this.delimiter = charsetName == null ? delimString.getBytes() : delimString.getBytes(charsetName);
		} catch (UnsupportedEncodingException useex) {
			throw new IllegalArgumentException("The charset with the name '" + charsetName + 
				"' is not supported on this TaskManager instance.", useex);
		}
		
		// set the number of samples
		this.numLineSamples = DEFAULT_NUM_SAMPLES;
		final String samplesString = parameters.getString(NUM_STATISTICS_SAMPLES, null);
		
		if (samplesString != null) {
			try {
				this.numLineSamples = Integer.parseInt(samplesString);
			}
			catch (NumberFormatException nfex) {
				if (LOG.isWarnEnabled())
					LOG.warn("Invalid value for number of samples to take: " + samplesString +
							". Using default value of " + DEFAULT_NUM_SAMPLES);
			}
		}
	}
	
	
	
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#getStatistics()
	 */
	@Override
	public FileBaseStatistics getStatistics(BaseStatistics cachedStatistics)
	{
		// check the cache
		FileBaseStatistics stats = null;
		
		if (cachedStatistics != null && cachedStatistics instanceof FileBaseStatistics) {
			stats = (FileBaseStatistics) cachedStatistics;
		}
		else {
			stats = new FileBaseStatistics(-1, BaseStatistics.UNKNOWN, BaseStatistics.UNKNOWN);
		}
		

		try {
			final Path file = this.filePath;
			final URI uri = file.toUri();

			// get the filesystem
			final FileSystem fs = FileSystem.get(uri);
			List<FileStatus> files = null;

			// get the file info and check whether the cached statistics are still
			// valid.
			{
				FileStatus status = fs.getFileStatus(file);

				if (status.isDir()) {
					FileStatus[] fss = fs.listStatus(file);
					files = new ArrayList<FileStatus>(fss.length);
					boolean unmodified = true;

					for (FileStatus s : fss) {
						if (!s.isDir()) {
							files.add(s);
							if (s.getModificationTime() > stats.getLastModificationTime()) {
								stats.fileModTime = s.getModificationTime();
								unmodified = false;
							}
						}
					}

					if (unmodified) {
						return stats;
					}
				}
				else {
					// check if the statistics are up to date
					long modTime = status.getModificationTime();	
					if (stats.getLastModificationTime() == modTime) {
						return stats;
					}

					stats.fileModTime = modTime;
					
					files = new ArrayList<FileStatus>(1);
					files.add(status);
				}
			}

			stats.avgBytesPerRecord = -1.0f;
			stats.fileSize = 0;
			
			// calculate the whole length
			for (FileStatus s : files) {
				stats.fileSize += s.getLen();
			}
			
			// sanity check
			if (stats.fileSize <= 0) {
				stats.fileSize = BaseStatistics.UNKNOWN;
				return stats;
			}
			

			// currently, the sampling only works on line separated data
			final byte[] delimiter = getDelimiter();
			if (! ((delimiter.length == 1 && delimiter[0] == '\n') ||
				   (delimiter.length == 2 && delimiter[0] == '\r' && delimiter[1] == '\n')) )
			{
				return stats;
			}
						
			// make the samples small for very small files
			int numSamples = Math.min(this.numLineSamples, (int) (stats.fileSize / 1024));
			if (numSamples < 2) {
				numSamples = 2;
			}

			long offset = 0;
			long bytes = 0; // one byte for the line-break
			long stepSize = stats.fileSize / numSamples;

			int fileNum = 0;
			int samplesTaken = 0;

			// take the samples
			for (int sampleNum = 0; sampleNum < numSamples && fileNum < files.size(); sampleNum++) {
				FileStatus currentFile = files.get(fileNum);
				FSDataInputStream inStream = null;

				try {
					inStream = fs.open(currentFile.getPath());
					LineReader lineReader = new LineReader(inStream, offset, currentFile.getLen() - offset, 1024);
					byte[] line = lineReader.readLine();
					lineReader.close();

					if (line != null && line.length > 0) {
						samplesTaken++;
						bytes += line.length + 1; // one for the linebreak
					}
				}
				finally {
					// make a best effort to close
					if (inStream != null) {
						try {
							inStream.close();
						} catch (Throwable t) {}
					}
				}

				offset += stepSize;

				// skip to the next file, if necessary
				while (fileNum < files.size() && offset >= (currentFile = files.get(fileNum)).getLen()) {
					offset -= currentFile.getLen();
					fileNum++;
				}
			}

			stats.avgBytesPerRecord = bytes / (float) samplesTaken;
		}
		catch (IOException ioex) {
			if (LOG.isWarnEnabled())
				LOG.warn("Could not determine complete statistics for file '" + filePath + "' due to an io error: "
						+ ioex.getMessage());
		}
		catch (Throwable t) {
			if (LOG.isErrorEnabled())
				LOG.error("Unexpected problen while getting the file statistics for file '" + filePath + "': "
						+ t.getMessage(), t);
		}

		return stats;
	}

	/**
	 * Opens the given input split. This method opens the input stream to the specified file, allocates read buffers
	 * and positions the stream at the correct position, making sure that any partial record at the beginning is skipped.
	 * 
	 * @param split The input split to open.
	 * 
	 * @see eu.stratosphere.pact.common.io.FileInputFormat#open(eu.stratosphere.nephele.fs.FileInputSplit)
	 */
	@Override
	public void open(FileInputSplit split) throws IOException
	{
		super.open(split);
		
		this.bufferSize = this.bufferSize <= 0 ? DEFAULT_READ_BUFFER_SIZE : this.bufferSize;
		this.readBuffer = new byte[this.bufferSize];
		this.wrapBuffer = new byte[256];

		this.readPos = 0;
		this.overLimit = false;
		this.end = false;

		if (this.splitStart != 0) {
			this.stream.seek(this.splitStart);
			readLine();
			
			// if the first partial record already pushes the stream over the limit of our split, then no
			// record starts within this split 
			if (this.overLimit) {
				this.end = true;
			}
		}
		else {
			fillBuffer();
		}
	}

	/**
	 * Checks whether the current split is at its end.
	 * 
	 * @return True, if the split is at its end, false otherwise.
	 */
	@Override
	public boolean reachedEnd()
	{
		return this.end;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#nextRecord(java.lang.Object)
	 */
	@Override
	public boolean nextRecord(Tuple record) throws IOException
	{
		if (readLine()) {
			return readRecord(record, this.currBuffer, this.currOffset, this.currLen);
		} else {
			this.end = true;
			return false;
		}
	}
	
	/**
	 * Closes the input by releasing all buffers and closing the file input stream.
	 * 
	 * @throws IOException Thrown, if the closing of the file stream causes an I/O error.
	 */
	@Override
	public void close() throws IOException
	{
		this.wrapBuffer = null;
		this.readBuffer = null;
		
		super.close();
	}

	// --------------------------------------------------------------------------------------------

	private boolean readLine() throws IOException
	{
		if (this.stream == null || this.overLimit) {
			return false;
		}

		int countInWrapBuffer = 0;

		/* position of matching positions in the delimiter byte array */
		int i = 0;

		while (true) {
			if (this.readPos >= this.limit) {
				if (!fillBuffer()) {
					if (countInWrapBuffer > 0) {
						setResult(this.wrapBuffer, 0, countInWrapBuffer);
						return true;
					} else {
						return false;
					}
				}
			}

			int startPos = this.readPos;
			int count = 0;

			while (this.readPos < this.limit && i < this.delimiter.length) {
				if ((this.readBuffer[this.readPos++]) == this.delimiter[i]) {
					i++;
				} else {
					i = 0;
				}

			}

			// check why we dropped out
			if (i == this.delimiter.length) {
				// line end
				count = this.readPos - startPos - this.delimiter.length;

				// copy to byte array
				if (countInWrapBuffer > 0) {
					// check wrap buffer size
					if (this.wrapBuffer.length < countInWrapBuffer + count) {
						final byte[] nb = new byte[countInWrapBuffer + count];
						System.arraycopy(this.wrapBuffer, 0, nb, 0, countInWrapBuffer);
						this.wrapBuffer = nb;
					}
					if (count >= 0) {
						System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, count);
					}
					setResult(this.wrapBuffer, 0, countInWrapBuffer + count);
					return true;
				} else {
					setResult(this.readBuffer, startPos, count);
					return true;
				}
			} else {
				count = this.limit - startPos;

				// buffer exhausted
				while (this.wrapBuffer.length - countInWrapBuffer < count) {
					// reallocate
					byte[] tmp = new byte[this.wrapBuffer.length * 2];
					System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
					this.wrapBuffer = tmp;
				}

				System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, count);
				countInWrapBuffer += count;
			}
		}
	}
	
	private final void setResult(byte[] buffer, int offset, int len) {
		this.currBuffer = buffer;
		this.currOffset = offset;
		this.currLen = len;
	}

	private final boolean fillBuffer() throws IOException {
		int toRead = this.splitLength > this.readBuffer.length ? this.readBuffer.length : (int) this.splitLength;
		if (this.splitLength <= 0) {
			toRead = this.readBuffer.length;
			this.overLimit = true;
		}

		int read = this.stream.read(this.readBuffer, 0, toRead);

		if (read == -1) {
			this.stream.close();
			this.stream = null;
			return false;
		} else {
			this.splitLength -= read;
			this.readPos = 0;
			this.limit = read;
			return true;
		}
	}
	
	// ============================================================================================
	
	/**
	 * Creates a configuration builder that can be used to set the input format's parameters to the config in a fluent
	 * fashion.
	 * 
	 * @return A config builder for setting parameters.
	 */
	public static ConfigBuilder configureDelimitedFormat(FileDataSource target) {
		return new ConfigBuilder(target.getParameters());
	}
	
	/**
	 * Abstract builder used to set parameters to the input format's configuration in a fluent way.
	 */
	protected static class AbstractConfigBuilder<T> extends SPigFileInputFormat.AbstractConfigBuilder<T>
	{
		private static final String NEWLINE_DELIMITER = "\n";
		
		// --------------------------------------------------------------------
		
		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param targetConfig The configuration into which the parameters will be written.
		 */
		protected AbstractConfigBuilder(Configuration config) {
			super(config);
		}
		
		// --------------------------------------------------------------------
		
		/**
		 * Sets the delimiter to be a single character, namely the given one. The character must be within
		 * the value range <code>0</code> to <code>127</code>.
		 * 
		 * @param delimiter The delimiter character.
		 * @return The builder itself.
		 */
		public T recordDelimiter(char delimiter) {
			if (delimiter == '\n') {
				this.config.setString(RECORD_DELIMITER, NEWLINE_DELIMITER);
			} else {
				this.config.setString(RECORD_DELIMITER, String.valueOf(delimiter));
			}
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
		
		/**
		 * Sets the delimiter to be the given string. The string will be converted to bytes for more efficient
		 * comparison during input parsing. The conversion will be done using the platforms default charset.
		 * 
		 * @param delimiter The delimiter string.
		 * @return The builder itself.
		 */
		public T recordDelimiter(String delimiter) {
			this.config.setString(RECORD_DELIMITER, delimiter);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
		
		/**
		 * Sets the delimiter to be the given string. The string will be converted to bytes for more efficient
		 * comparison during input parsing. The conversion will be done using the charset with the given name.
		 * The charset must be available on the processing nodes, otherwise an exception will be raised at
		 * runtime.
		 * 
		 * @param delimiter The delimiter string.
		 * @param charsetName The name of the encoding character set.
		 * @return The builder itself.
		 */
		public T recordDelimiter(String delimiter, String charsetName) {
			this.config.setString(RECORD_DELIMITER, delimiter);
			this.config.setString(RECORD_DELIMITER_ENCODING, charsetName);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
		
		/**
		 * Sets the number of line samples to take in order to estimate the base statistics for the
		 * input format.
		 * 
		 * @param numSamples The number of line samples to take.
		 * @return The builder itself.
		 */
		public T numSamplesForStatistics(int numSamples) {
			this.config.setInteger(NUM_STATISTICS_SAMPLES, numSamples);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
	}
	
	/**
	 * A builder used to set parameters to the input format's configuration in a fluent way.
	 */
	public static class ConfigBuilder extends AbstractConfigBuilder<ConfigBuilder>
	{
		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param targetConfig The configuration into which the parameters will be written.
		 */
		protected ConfigBuilder(Configuration targetConfig) {
			super(targetConfig);
		}
		
	}

}
