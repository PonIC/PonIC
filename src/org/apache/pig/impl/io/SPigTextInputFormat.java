package org.apache.pig.impl.io;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.base.PactString;


/**
 * 
 * Copy of TextInputFormat reading Tuples instead of PactRecords
 * @author hinata
 *
 */
public class SPigTextInputFormat extends SPigDelimitedInputFormat {

public static final String CHARSET_NAME = "textformat.charset";
	
	public static final String DEFAULT_CHARSET_NAME = "UTF-8";
	
	private static final Log LOG = LogFactory.getLog(SPigDelimitedInputFormat.class);
	
	
	protected final PactString theString = new PactString();
	
	protected CharsetDecoder decoder;
	
	protected ByteBuffer byteWrapper;
	
	protected boolean ascii;
	
	@Override
	public void configure(Configuration parameters)
	{
		super.configure(parameters);
		
		// get the charset for the decoding
		String charsetName = parameters.getString(CHARSET_NAME, DEFAULT_CHARSET_NAME);
		if (charsetName == null || !Charset.isSupported(charsetName)) {
			throw new RuntimeException("Unsupported charset: " + charsetName);
		}
		
		if (charsetName.equals("ISO-8859-1") || charsetName.equalsIgnoreCase("ASCII")) {
			this.ascii = true;
		} else {
			this.decoder = Charset.forName(charsetName).newDecoder();
			this.byteWrapper = ByteBuffer.allocate(1);
		}
	}
	@Override
	//TODO: Find where will this target Tuple be instantiated???
	public boolean readRecord(Tuple target, byte[] bytes, int offset,
			int numBytes) {
		
		PactString str = this.theString;
		
		if (this.ascii) {
			str.setValueAscii(bytes, offset, numBytes);
		}
		else {
			ByteBuffer byteWrapper = this.byteWrapper;
			if (bytes != byteWrapper.array()) {
				byteWrapper = ByteBuffer.wrap(bytes, 0, bytes.length);
				this.byteWrapper = byteWrapper;
			}
			byteWrapper.clear();
			byteWrapper.position(offset);
			byteWrapper.limit(offset + numBytes);
				
			try {
				CharBuffer result = this.decoder.decode(byteWrapper);
				str.setValue(result);
			}
			catch (CharacterCodingException e) {
				byte[] copy = new byte[numBytes];
				System.arraycopy(bytes, offset, copy, 0, numBytes);
				LOG.warn("Line could not be encoded: " + Arrays.toString(copy), e);
				return false;
			}
		}
		
		target.setNull(true);
		//target.clear();
		try {
			target.set(0, str.getValue());
		} catch (ExecException e) {
			e.printStackTrace();
		}
		return true;
	}

}
