import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
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
public class SJoin implements PlanAssembler {

	
	/**
	 * Identity Join
	 * @author hinata
	 *
	 */
	public static class Join extends MatchStub{

		PactRecord joined = new PactRecord(2);
		@Override
		public void match(PactRecord rec1, PactRecord rec2, Collector<PactRecord> collector)
				throws Exception {
			joined.setField(0, rec1.getField(2, PactString.class));
			
			joined.setField(1, rec2.getField(2, PactString.class));
						
			collector.collect(joined);
		}	
		
	}

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
			this.buffer.append('\t');
			this.buffer.append(record.getField(1, PactString.class).toString());
			this.buffer.append('\n');
			
			
			byte[] bytes = this.buffer.toString().getBytes();
			this.stream.write(bytes);
		}
	}

	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String in1 = (args.length > 1 ? args[1] : "");
		String in2 = (args.length > 2 ? args[2] : "");
		String out = (args.length > 3 ? args[3] : "");

		FileDataSource src1 = new FileDataSource(RecordInputFormat.class, in1, "source1");
		src1.setParameter(RecordInputFormat.RECORD_DELIMITER, "\n");
		src1.setParameter(RecordInputFormat.NUM_FIELDS_PARAMETER, 3);
		src1.setParameter(RecordInputFormat.FIELD_DELIMITER_PARAMETER, " ");
		src1.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+0, DecimalTextIntParser.class);
		src1.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+1, DecimalTextIntParser.class);
		src1.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+2, VarLengthStringParser.class);
		
		
		FileDataSource src2 = new FileDataSource(RecordInputFormat.class, in2, "source2");
		src2.setParameter(RecordInputFormat.RECORD_DELIMITER, "\n");
		src2.setParameter(RecordInputFormat.NUM_FIELDS_PARAMETER, 3);
		src2.setParameter(RecordInputFormat.FIELD_DELIMITER_PARAMETER, " ");
		src2.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+0, DecimalTextIntParser.class);
		src2.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+1, DecimalTextIntParser.class);
		src2.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+2, VarLengthStringParser.class);
		
		
		MatchContract join = MatchContract.builder(Join.class, PactInteger.class, 1, 1)
				.input1(src1)
				.input2(src2)
				.name("join")
				.build();
		
		FileDataSink sink = new FileDataSink(TestOutFormat.class, out, join, "sink");

		Plan plan = new Plan(sink, "join");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

}