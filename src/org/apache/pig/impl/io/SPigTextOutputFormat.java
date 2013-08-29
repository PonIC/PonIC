package org.apache.pig.impl.io;

import java.io.IOException;

import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class SPigTextOutputFormat extends FileOutputFormat {

	private final StringBuilder buffer = new StringBuilder();
	
	@Override
	public void writeRecord(PactRecord record) throws IOException {
		this.buffer.setLength(0);
		this.buffer.append(record.getField(0, PactString.class).toString());
		this.buffer.append('\n');

		byte[] bytes = this.buffer.toString().getBytes();
		this.stream.write(bytes);
	}

}
