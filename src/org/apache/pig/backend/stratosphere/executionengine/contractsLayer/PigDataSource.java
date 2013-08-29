package org.apache.pig.backend.stratosphere.executionengine.contractsLayer;

import org.apache.pig.impl.io.SPigFileInputFormat;
import org.apache.pig.impl.io.SPigTextInputFormat;

import eu.stratosphere.pact.common.contract.GenericDataSource;

public class PigDataSource extends GenericDataSource<SPigTextInputFormat> {

	private final String inputFile;
	
	public PigDataSource(Class<? extends SPigTextInputFormat> clazz) {
		super(clazz);
		inputFile = "";
	}
	
	public PigDataSource(Class<? extends SPigTextInputFormat> clazz, String inputFile) {
		super(clazz);
		this.inputFile = inputFile;
		this.parameters.setString(SPigFileInputFormat.FILE_PARAMETER_KEY, inputFile);
	}
	
	/**
	 * Returns the file path from which the input is read.
	 * 
	 * @return The path from which the input shall be read.
	 */
	public String getFilePath()
	{
		return this.inputFile;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString()
	{
		return this.inputFile;
	}

}
