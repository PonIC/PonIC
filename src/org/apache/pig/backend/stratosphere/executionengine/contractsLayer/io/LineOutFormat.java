package org.apache.pig.backend.stratosphere.executionengine.contractsLayer.io;

import java.io.IOException;

import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class LineOutFormat extends FileOutputFormat {

	private final StringBuilder buffer = new StringBuilder();

	@Override
	public void writeRecord(PactRecord record) throws IOException {
		this.buffer.setLength(0);
		for(int i=0; i<record.getNumFields(); i++){
			this.buffer.append(record.getField(i, PactString.class).toString() + " ");
		}
		this.buffer.append('\n');

		byte[] bytes = this.buffer.toString().getBytes();
		this.stream.write(bytes);
	}

}
