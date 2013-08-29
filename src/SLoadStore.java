
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.parser.*;

/**
 * Simple Load-Store Pact Program
 * @author vkalavri
 *
 */
public class SLoadStore implements PlanAssembler {

	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String in = (args.length > 1 ? args[1] : "");
		String out = (args.length > 2 ? args[2] : "");

		FileDataSource source = new FileDataSource(RecordInputFormat.class, in, "in");		
		
		source.setParameter(RecordInputFormat.RECORD_DELIMITER, "\n");
		source.setParameter(RecordInputFormat.NUM_FIELDS_PARAMETER, 7);
		source.setParameter(RecordInputFormat.FIELD_DELIMITER_PARAMETER, "\u0001");
		source.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+0, VarLengthStringParser.class);
		source.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+1, DecimalTextIntParser.class);
		source.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+2, DecimalTextIntParser.class);
		source.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+3, VarLengthStringParser.class);
		source.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+4, DecimalTextLongParser.class);
		source.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+5, DecimalTextLongParser.class);
		source.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+6, DecimalTextDoubleParser.class);
		
		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, out, source, "out");
		sink.setParameter(RecordOutputFormat.NUM_FIELDS_PARAMETER, 7);
		sink.setParameter(RecordOutputFormat.FIELD_DELIMITER_PARAMETER, " ");
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX+0, PactString.class);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX+1, PactInteger.class);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX+2, PactInteger.class);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX+3, PactString.class);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX+4, PactLong.class);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX+5, PactLong.class);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX+6, PactDouble.class);
		
		Plan plan = new Plan(sink, "Load_Store");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

}