/**
 * 
 * NOTE: This class needs to be copied to Pig's home folder
 * so that it is in the root directory of the jar which will be created 
 * for submission to Nephele
 * 
 */


import java.io.File;
import java.util.Properties;
import org.apache.pig.backend.stratosphere.executionengine.contractsLayer.PactCompiler;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.util.UDFContext;

import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;


public class PigPlanAssemblerExt implements PlanAssembler {
	
	@Override
	public Plan getPlan(String... args) {
		
		UDFContext udfCon = UDFContext.getUDFContext();		
		Properties properties = new Properties();
        PropertiesUtil.loadDefaultProperties(properties);
        PropertiesUtil.loadPropertiesFromFile(properties, System.getProperty("user.dir" + File.separator 
        		+ "conf/pig.properties"));
        PigContext pc = PropertiesUtil.loadPigContext(properties.getProperty("pig.context.filepath"));
        PactPlan pp = PropertiesUtil.loadPactPlan(properties.getProperty("pig.pactplan.filepath"));
		
		PactCompiler pactComp = new PactCompiler(pp, pc, udfCon);
  		pactComp.compilePlan();
  				  		
		  		
		// parse job parameters
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		
	
		//create the plan to submit
		Plan plan = new Plan(pactComp.getSink(), "Pig Stratosphere Job");
		
		//set degree of parallelism
		plan.setDefaultParallelism(noSubTasks);			
		
		return plan;
	}
	
}
