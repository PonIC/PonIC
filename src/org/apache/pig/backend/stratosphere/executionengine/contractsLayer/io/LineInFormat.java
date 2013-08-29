package org.apache.pig.backend.stratosphere.executionengine.contractsLayer.io;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class LineInFormat extends TextInputFormat {

	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
		String str = new String(bytes, offset, numBytes);
		String[] parts = str.split("\\s+");
		
		for (int i=0; i<parts.length; i++){
			target.setField(i, new PactString(parts[i]));				
		}

		return true;
	}
}
