import java.io.IOException;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.*;

/**
 * A test Pact Program to filter out 10% of the input
 * @author hinata
 *
 */
public class SFilter50 implements PlanAssembler {

	public static class LineInFormat50 extends DelimitedInputFormat
	{
		private final PactString string = new PactString();
		String [] tokens;
		
		@Override
		public boolean readRecord(PactRecord record, byte[] line, int offset, int numBytes)
		{
			this.string.setValueAscii(line, 0, numBytes);
			tokens = this.string.toString().split("\u0001");
			
			//filter 50%
			int i=(int) (Math.random()*2);
			if(i < 1){
				return false;
			}
			else{
				record.setField(0, this.string);
				return true;
			}
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
			this.buffer.append('\n');
			
			byte[] bytes = this.buffer.toString().getBytes();
			this.stream.write(bytes);
		}
	}

	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String in = (args.length > 1 ? args[1] : "");
		String out = (args.length > 2 ? args[2] : "");

		FileDataSource src = new FileDataSource(LineInFormat50.class, in, "source");
		
		FileDataSink sink = new FileDataSink(TestOutFormat.class, out, src, "sink");

		Plan plan = new Plan(sink, "filter 90%");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

}