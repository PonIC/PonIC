package org.apache.pig.backend.stratosphere.executionengine.contractsLayer.stubs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.stratosphere.executionengine.util.DataTypeUtils;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class PigStubs {


	public static class PigMapStub extends PigGenericMapStub
	{
			
		@Override
		public void map(PactRecord record, Collector<PactRecord> collector)
		{
					
			if(pactOp != null){
				try {
					//convert record to a Pig Tuple
					pactOp.attachInput(DataTypeUtils.recordToTuple(record));
					runPipeline();
					
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			if(outputRecord.getNumFields() > 0)
				collector.collect(outputRecord);
		}	
	}
	
	public static class PigReduceStub extends PigGenericReduceStub
	{
				
		/*
		 * TODO: needs to package the list of PactRecords into a Pig Bag
		 * and call runPipeline() on some kind of package operator
		 * 
		 * this one is a simple grouping
		 * 
		 */
		@Override
		public void reduce(Iterator<PactRecord> iter, Collector<PactRecord> collector)
				throws Exception {
						
			int numFields;
			int keyPos;
			PactRecord rec;
			List<String> str = new ArrayList<String>();
			
			if(pactOp != null){
				keyPos = pactOp.getFirstKeyPosition();
				PactRecord first = iter.next();
				int firstNumFields = first.getNumFields();
				outputRecord.setField(0, new PactString(first.getField(keyPos, PactString.class)));
				for(int i=0; i < firstNumFields; i++){
					if(i != keyPos) {
						str.add(first.getField(i, PactString.class).toString());
					}
				}
				
				while(iter.hasNext()){
					rec = iter.next();
					numFields = rec.getNumFields();
					for(int i=0; i < numFields; i++) {
						if(i != keyPos)
							str.add(rec.getField(i, PactString.class).toString());
					}
				}
								
				outputRecord.setNumFields(str.size());
				for(int i=1; i<str.size()+1; i++) {
					outputRecord.setField(i, new PactString(str.get(i-1)));
				}
				
				if(outputRecord.getNumFields() > 0)
					collector.collect(outputRecord);
			}
		}
		
	}
	
	public static class PigMatchStub extends PigGenericMatchStub
	{
		
		@Override
		public void match(PactRecord rec1, PactRecord rec2, Collector<PactRecord> collector) 
				throws Exception {
			
			for (int i=0; i<rec1.getNumFields(); i++) {
				outputRecord.setField(i, rec1.getField(i, PactString.class));
			}
			for (int j=rec1.getNumFields(); j<rec1.getNumFields()+rec2.getNumFields(); j++) {
				outputRecord.setField(j, rec2.getField(j-rec1.getNumFields(), PactString.class));
			}
						
			collector.collect(outputRecord);			
		}
	}
		
		public static class PigCrossStub extends PigGenericCrossStub
		{

			@Override
			public void cross(PactRecord rec1, PactRecord rec2, Collector<PactRecord> collector) {
				for (int i=0; i<rec1.getNumFields(); i++) {
					outputRecord.setField(i, rec1.getField(i, PactString.class));
				}
				for (int j=rec1.getNumFields(); j<rec1.getNumFields()+rec2.getNumFields(); j++) {
					outputRecord.setField(j, rec2.getField(j-rec1.getNumFields(), PactString.class));
				}
							
				collector.collect(outputRecord);
			}
		
		}
	
	
		public static class PigCoGroupStub extends PigGenericCoGroupStub
		{

			@Override
			public void coGroup(Iterator<PactRecord> it1, Iterator<PactRecord> it2, 
					Collector<PactRecord> collector) {
				
			}			
		
		}
		
}

