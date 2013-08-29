package org.apache.pig.impl.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.SLoadFunc;
import org.apache.pig.backend.stratosphere.executionengine.contractsLayer.PigDataSource;
import org.apache.pig.builtin.SPigStorage;
import org.apache.pig.data.Tuple;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.library.FileLineWriter;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.generic.io.InputFormat;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;

/**
 * Wrapper Loader which wraps a real SLoadFunc underneath and allows
 * to read a file completely starting a given split 
 * 
 * Based on org.apache.pig.impl.io.ReadToEndLoader
 * 
 * The call sequence to use this is:
 * 1) construct an object using the constructor
 * 2) Call getNext() in a loop till it returns null
 */
public class SReadToEndLoader extends SLoadFunc {

	 /**
     * the wrapped LoadFunc which will do the actual reading
     */
    private SLoadFunc wrappedLoadFunc;
    private Configuration conf;
    /**
     * the input location string (typically input file/dir name )
     */
    private String inputLocation;
      
    /**
     * If the splits to be read are not in increasing sequence of integers
     * this array can be used
     */
    private int[] toReadSplits = null;
    
    /**
     * index into toReadSplits
     */
    private int toReadSplitsIdx = 0;
    
    /**
     * the index of the split the loader is currently reading from
     */
    private int curSplitIndex;
    /**
     * the input splits returned by underlying {@link InputFormat#getSplits(JobContext)}
     */
    private List<FileInputSplit> inpSplits = null;
    
    /**
     * underlying RecordReader
     */
    private RecordReader reader = null;
    
    /**
     * underlying InputFormat
     */
    private SPigTextInputFormat inputFormat = null;
    
    public SReadToEndLoader(String inputLocation, int splitIndex) throws IOException {
    	this.inputLocation = inputLocation;
        this.curSplitIndex = splitIndex;
        init();
    }
    /**
     * @param wrappedLoadFunc
     * @param conf
     * @param inputLocation
     * @param splitIndex
     * @throws IOException 
     * @throws InterruptedException 
     */
    public SReadToEndLoader(SLoadFunc wrappedLoadFunc, Configuration conf,
            String inputLocation, int splitIndex) throws IOException {
        this.wrappedLoadFunc = wrappedLoadFunc;
        this.inputLocation = inputLocation;
        this.conf = conf;
        this.curSplitIndex = splitIndex;
        init();
    }
    
    /**
     * @param wrappedLoadFunc
     * @param inputLocation
     * @param splitIndex
     * @throws IOException 
     * @throws InterruptedException 
     */
    public SReadToEndLoader(SLoadFunc wrappedLoadFunc, String inputLocation, int splitIndex) throws IOException {
        this.wrappedLoadFunc = wrappedLoadFunc;
        this.inputLocation = inputLocation;
        this.curSplitIndex = splitIndex;
        init();
    }
    
    /**
     * This constructor takes an array of split indexes (toReadSplitIdxs) of the 
     * splits to be read.
     * @param wrappedLoadFunc
     * @param conf
     * @param inputLocation
     * @param toReadSplitIdxs
     * @throws IOException 
     * @throws InterruptedException 
     */
    public SReadToEndLoader(SLoadFunc wrappedLoadFunc, Configuration conf,
            String inputLocation, int[] toReadSplitIdxs) throws IOException {
        this.wrappedLoadFunc = wrappedLoadFunc;
        this.inputLocation = inputLocation;
        this.toReadSplits = toReadSplitIdxs;
        this.conf = conf;
        this.curSplitIndex =
            toReadSplitIdxs.length > 0 ? toReadSplitIdxs[0] : Integer.MAX_VALUE;
        init();
    }
 
	/**
     * Data is read by the Stratosphere DataSource PigDataSource
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private void init() throws IOException{
    }


	@Override
	public FileInputFormat getInputFormat() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Tuple getNext() throws IOException {
		
     Tuple t = wrappedLoadFunc.getNext();
     if(t != null)
    	 return t;
     else
    	 throw new IOException("[AVK] SReadToEndLoader: read empty tuple");
    
	}

	/*
    private Tuple getNextHelper() throws IOException, InterruptedException {
        Tuple t = null;
        while(initializeReader()) {
            t = wrappedLoadFunc.getNext();
            if(t == null) {
                // try next split
                updateCurSplitIndex();
            } else {
                return t;
            }
        }
        return null;
    }*/
    
    /**
     * Updates curSplitIndex , just increment if splitIndexes is null,
     * else get next split in splitIndexes
     */
    /*private void updateCurSplitIndex() {
        if(toReadSplits == null){
            ++curSplitIndex;
        }else{
            ++toReadSplitsIdx;
            if(toReadSplitsIdx >= toReadSplits.length){
                // finished all the splits in splitIndexes array
                curSplitIndex = Integer.MAX_VALUE;
            }else{
                curSplitIndex = toReadSplits[toReadSplitsIdx];
            }
        }
    }*/
    
    /*private boolean initializeReader() throws IOException, 
    InterruptedException {
	    if(curSplitIndex > inpSplits.size() - 1) {
	        // past the last split, we are done
	        return false;
	    }
	    InputSplit curSplit = inpSplits.get(curSplitIndex);
	    FileLineWriter fw = new FileLineWriter();
	    reader = new RecordReader(fw, Record.class);
        wrappedLoadFunc.prepareToRead(reader, curSplit);
        return true;
    }*/

	@Override
	public void prepareToRead(RecordReader reader, InputSplit split)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setLocation(String location, PigDataSource fds) throws IOException {
		// do nothing

	}
	
	@Override
    public PigDataSource getDataSource(){
		return this.wrappedLoadFunc.getDataSource();
	}

}
