package org.apache.pig.backend.stratosphere.executionengine.contractsLayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.stratosphere.executionengine.contractsLayer.io.LineInFormat;
import org.apache.pig.backend.stratosphere.executionengine.contractsLayer.io.LineOutFormat;
import org.apache.pig.backend.stratosphere.executionengine.contractsLayer.plans.ScalarPactFinder;
import org.apache.pig.backend.stratosphere.executionengine.contractsLayer.plans.UDFFinder;
import org.apache.pig.backend.stratosphere.executionengine.contractsLayer.stubs.PigStubs.PigCrossStub;
import org.apache.pig.backend.stratosphere.executionengine.contractsLayer.stubs.PigStubs.PigMapStub;
import org.apache.pig.backend.stratosphere.executionengine.contractsLayer.stubs.PigStubs.PigReduceStub;
import org.apache.pig.backend.stratosphere.executionengine.contractsLayer.stubs.PigStubs.PigMatchStub;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.PactOperator;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlan;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlanVisitor;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOCoGroup;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOCross;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOFilter;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOLoad;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOMatch;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOReduce;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOStore;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.PigException;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.type.base.PactString;

public class PactCompiler extends PactPlanVisitor {

	PactPlan plan;
    Contract[] compiledInputs = null;
    private Map<PactOperator, Contract> pactOpToContract;
    private String scope;
    private Random r;
    NodeIdGenerator nig;
    private static final Log log = LogFactory.getLog(PactLauncher.class);
    PactOperator curOp;	//the current operator being compiled
    private UDFFinder udfFinder;
    private GenericDataSink sink;
    PigContext pc;
  
    
    public PactCompiler(PactPlan plan, PigContext pc, UDFContext udfCon) {
        super(plan, new DepthFirstWalker<PactOperator, PactPlan>(plan));
        this.plan = plan;
        this.pc = pc;
        UDFContext.setUdfContext(udfCon);
        compiledInputs = new Contract[plan.size()];	//each PactOperator will become one InputContract
        nig = NodeIdGenerator.getGenerator();
        r = new Random(1331);
        FileLocalizer.setR(r);
        List<PactOperator> roots = plan.getRoots();
        if((roots == null) || (roots.size() <= 0)) {
        	String msg = "Internal error. Did not find roots in the pact plan.";
        	log.info(msg);
        }
        scope = roots.get(0).getOperatorKey().getScope();
        pactOpToContract = new HashMap<PactOperator, Contract>();
        udfFinder = new UDFFinder();
    }
    

	public void compilePlan() {
		try {
			this.visit();
		} catch (VisitorException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void visitLoad(SOLoad ld) throws VisitorException{
		try{
			curOp = ld;
			String inputLocation = ld.getInputFile();
			ld.setPc(this.pc);
        	FileDataSource source = new FileDataSource(LineInFormat.class, inputLocation, "Pig Source:-"+inputLocation);  
            pactOpToContract.put(ld, source);
            
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + ld.getClass().getSimpleName();
            throw new VisitorException(msg, errCode, PigException.BUG, e);
        }
    }


	@Override
	public void visitFilter(SOFilter soFilter) throws VisitorException {
		curOp = soFilter;
		try{
			processUDFs(soFilter.getPlan());
		}catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + soFilter.getClass().getSimpleName();
            throw new VisitorException(msg, errCode, PigException.BUG, e);
		}
		
		List<PactOperator> preds = soFilter.getInputs();
        if (!preds.isEmpty()){
			MapContract mapper = MapContract.builder(PigMapStub.class)
					.name("Filter Mapper")
					.build();
			
			//send parameters to the map stub through the Configuration
			try {
				mapper.getParameters().setString("pactOp", ObjectSerializer.serialize(soFilter));
				} catch (IOException e) {
				e.printStackTrace();
			}
			
			 pactOpToContract.put(soFilter, mapper);
        }
		
	}
	
	@Override
	public void visitCross(SOCross soCross) {
		curOp = soCross;
		List<PactOperator> preds = soCross.getInputs();

        if (!preds.isEmpty()){
	        CrossContract cross = CrossContract.builder(PigCrossStub.class)
	        		.name("cross")
	        		.build();
			
	      //send parameters to the match stub through the Configuration
			try {
				cross.getParameters().setString("pactOp", ObjectSerializer.serialize(soCross));
				} catch (IOException e) {
				e.printStackTrace();
			}
			pactOpToContract.put(soCross, cross);
        }	
	}
	
	@Override
	public void visitStore(SOStore st) throws VisitorException{
		try{
        	String out = st.getSFile().getFileName();     	
            List<PactOperator> preds = st.getInputs();
            
            if (!preds.isEmpty()){
            	
            	FileDataSink sink = new FileDataSink(LineOutFormat.class, out, "Pig Data Sink");
            	pactOpToContract.put(st, sink);
            	//NOTE: this assumes that we only allow one STORE command in the script
            	// should be generalized
            	this.sink = sink;
            }

        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + st.getClass().getSimpleName();
            throw new VisitorException(msg, errCode, PigException.BUG, e);
        }		
	}

	@Override
	public void visitMatch(SOMatch soMatch) {
		curOp = soMatch;
		List<PactOperator> preds = soMatch.getInputs();

        if (!preds.isEmpty()){
	        MatchContract match = MatchContract.builder(PigMatchStub.class, PactString.class, 
	        		soMatch.getFirstKeyPosition(), soMatch.getSecondKeyPosition())
	        		.name("match")
	        		.build();
			
	      //send parameters to the match stub through the Configuration
			try {
				match.getParameters().setString("pactOp", ObjectSerializer.serialize(soMatch));
				} catch (IOException e) {
				e.printStackTrace();
			}
			pactOpToContract.put(soMatch, match);
        }
	}

	@Override
	public void visitReduce(SOReduce soReduce) {
		curOp = soReduce;			
		List<PactOperator> preds = soReduce.getInputs();
        if (!preds.isEmpty()){

        	//create the Reduce Input Contract
        	ReduceContract reducer = ReduceContract.builder(PigReduceStub.class, PactString.class, soReduce.getFirstKeyPosition())
        			.name("Reducer")
        			.build();
			//send parameters to the reduce stub through the Configuration
			try {
				reducer.getParameters().setString("pactOp", ObjectSerializer.serialize(soReduce));
				} catch (IOException e) {
				e.printStackTrace();
			}
			
			 pactOpToContract.put(soReduce, reducer);
        }
	}
	
	@Override
	public void visitCoGroup(SOCoGroup soCoGroup) {
		
	}
	
	private void processUDFs(PactPlan plan) throws VisitorException{
        if(plan!=null){
            //Process Scalar UDFs with referencedOperators
            ScalarPactFinder scalarPactFinder = new ScalarPactFinder(plan);
            scalarPactFinder.visit();
            curOp.scalars.addAll(scalarPactFinder.getScalars());
            
            //Process UDFs
            udfFinder.setPlan(plan);
            udfFinder.visit();
            curOp.UDFs.addAll(udfFinder.getUDFs());
        }
    }
	
	public GenericDataSink getSink(){
		Set<Entry<PactOperator, Contract>> ops = pactOpToContract.entrySet();
		Iterator<Entry<PactOperator, Contract>> iter = ops.iterator();
		
		while(iter.hasNext()){
			Entry<PactOperator, Contract> entry = iter.next();
			Contract curCon = entry.getValue();
			PactOperator curOp = entry.getKey();
			
			List<PactOperator> preds = curOp.getInputs();
			List<Contract> inputs = new ArrayList<Contract>();
			
			//Map Contract
			if (curCon.getClass() == MapContract.class){
				
				if (!preds.isEmpty()){
			        for(PactOperator po : preds){
			        	inputs.add(pactOpToContract.get(po));
			        }  	
		        }
				
				MapContract map = (MapContract)curCon;
				map.addInput(inputs.get(0));
			}
			
			//Reducer Contract
			if (curCon.getClass() == ReduceContract.class){
				
				if (!preds.isEmpty()){
			        for(PactOperator po : preds){
			        	inputs.add(pactOpToContract.get(po));
			        }  	
		        }
				
				ReduceContract reduce = (ReduceContract)curCon;
				reduce.addInput(inputs.get(0));
			}
			
			//Match Contract
			if (curCon.getClass() == MatchContract.class){
				
				if (!preds.isEmpty()){
			        for(PactOperator po : preds){
			        	inputs.add(pactOpToContract.get(po));
			        }  	
		        }
				
				MatchContract match = (MatchContract)curCon;
				match.addFirstInput(inputs.get(0));
				match.addSecondInput(inputs.get(1));
			}
			
			//Cross Contract
			if (curCon.getClass() == CrossContract.class){
				
				if (!preds.isEmpty()){
			        for(PactOperator po : preds){
			        	inputs.add(pactOpToContract.get(po));
			        }  	
		        }
				
				CrossContract cross = (CrossContract)curCon;
				cross.addFirstInput(inputs.get(0));
				cross.addSecondInput(inputs.get(1));
			}
			
			//Sink
			if (curCon == this.sink){
				
				if (!preds.isEmpty()){
			        for(PactOperator po : preds){
			        	inputs.add(pactOpToContract.get(po));
			        }  	
		        }
				
				this.sink.addInput(inputs.get(0));
			}
		}
		
		return sink;
	}
		
}
