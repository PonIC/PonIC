import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextDoubleParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextLongParser;
import eu.stratosphere.pact.common.type.base.parser.VarLengthStringParser;

/**
 * A test Pact Program to filter out 10% of the input
 * @author hinata
 *
 */
public class SGroup implements PlanAssembler {

	/**
	 * Writes PactRecord containing a string to a file.
	 * The output format is: "word\n"
	 */
	public static class TestOutFormat extends FileOutputFormat
	{
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
	/**
	 * Identity Reducer
	 * @author hinata
	 *
	 */
	public static class GroupReducer extends ReduceStub{

		@Override
		public void reduce(Iterator<PactRecord> iter, Collector<PactRecord> collector)
				throws Exception {
			
			PactRecord outputRecord = new PactRecord();
			PactRecord rec;
			String str = "";
				
			while(iter.hasNext()){
				rec = iter.next();
				str = str + " " + rec.getField(0, PactInteger.class).toString();
				str = str + " " + rec.getField(1, PactInteger.class).toString();
				str = str + " " + rec.getField(2, PactString.class).toString();
			}
					
			outputRecord.setNumFields(1);
			outputRecord.setField(0, new PactString(str));
			collector.collect(outputRecord);
				
		}		
		
	}


	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String in = (args.length > 1 ? args[1] : "");
		String out = (args.length > 2 ? args[2] : "");

		FileDataSource src = new FileDataSource(RecordInputFormat.class, in, "source1");
		src.setParameter(RecordInputFormat.RECORD_DELIMITER, "\n");
		src.setParameter(RecordInputFormat.NUM_FIELDS_PARAMETER, 3);
		src.setParameter(RecordInputFormat.FIELD_DELIMITER_PARAMETER, " ");
		src.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+0, DecimalTextIntParser.class);
		src.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+1, DecimalTextIntParser.class);
		src.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+2, VarLengthStringParser.class);
		
		ReduceContract group = ReduceContract.builder(GroupReducer.class)
				.keyField(PactInteger.class, 1)
				.input(src)
				.name("group")
				.build();
		
		FileDataSink sink = new FileDataSink(TestOutFormat.class, out, group, "sink");

		Plan plan = new Plan(sink, "group");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

}