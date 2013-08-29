/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.newplan.logical.relational;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogicalToPhysicalTranslatorException;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.PactOperator;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlan;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOCoGroup;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOCross;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOFilter;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOLoad;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOMatch;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOReduce;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOStore;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.util.CompilerUtils;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.ReverseDependencyOrderWalkerWOSeenChk;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.expression.ExpToPactTranslationVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup.GROUPTYPE;

public class LogToPactTranslationVisitor extends LogicalRelationalNodesVisitor {
    
    public LogToPactTranslationVisitor(OperatorPlan plan) throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
        currentPlan = new PactPlan();
        logToPhyMap = new HashMap<Operator, PactOperator>();
        currentPlans = new Stack<PactPlan>();
    }
    
    protected final Log log = LogFactory.getLog(getClass());

    protected Map<Operator, PactOperator> logToPhyMap;

    protected Stack<PactPlan> currentPlans;

    protected PactPlan currentPlan;

    protected NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();

    protected PigContext pc;
        
    public void setPigContext(PigContext pc) {
        this.pc = pc;
    }

    public Map<Operator, PactOperator> getLogToPhyMap() {
        return logToPhyMap;
    }
    
    public PactPlan getPhysicalPlan() {
        return currentPlan;
    }
    
    @Override
    public void visit(LOLoad loLoad) throws FrontendException {
        String scope = DEFAULT_SCOPE;

        SOLoad load = new SOLoad(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), loLoad.getLoadFunc(), loLoad.getFileSpec().getFileName());
        load.setAlias(loLoad.getAlias());
        load.setLFile(loLoad.getFileSpec());
        load.setPc(pc);
        load.setResultType(DataType.BAG);
        load.setSignature(loLoad.getSignature());
        load.setLimit(loLoad.getLimit());
        currentPlan.add(load);
        logToPhyMap.put(loLoad, load);

        // Load is typically a root operator, but in the multiquery
        // case it might have a store as a predecessor.
        List<Operator> op = loLoad.getPlan().getPredecessors(loLoad);
        PactOperator from;
        
        if(op != null) {
            from = logToPhyMap.get(op.get(0));
            try {
                currentPlan.connect(from, load);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid pact operators in the pact plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
    }    

    @Override
    public void visit(LOFilter filter) throws FrontendException {
    	String scope = DEFAULT_SCOPE;
        SOFilter soFilter = new SOFilter(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), filter.getRequestedParallelisam());
        soFilter.setAlias(filter.getAlias());
        soFilter.setResultType(DataType.BAG);
        currentPlan.add(soFilter);
        logToPhyMap.put(filter, soFilter);
        currentPlans.push(currentPlan);   
        currentPlan = new PactPlan();
        PlanWalker childWalker = new ReverseDependencyOrderWalkerWOSeenChk(filter.getFilterPlan());
        pushWalker(childWalker);
        
        currentWalker.walk(
                new ExpToPactTranslationVisitor( currentWalker.getPlan(), 
                        childWalker, filter, currentPlan, logToPhyMap ) );
        
        popWalker();
        soFilter.setPlan(currentPlan);
        currentPlan = currentPlans.pop();

        List<Operator> op = filter.getPlan().getPredecessors(filter);

        PactOperator from;
        if(op != null) {
            from = logToPhyMap.get(op.get(0));
        } else {
            int errCode = 2051;
            String msg = "Did not find a predecessor for Filter." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);
        }
        
        try {
            currentPlan.connect(from, soFilter);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
        translateSoftLinks(filter);
    }
    
    @Override
    public void visit(LOCross cross) throws FrontendException {
        String scope = DEFAULT_SCOPE;
        
        // List of cross predicates 
        List<Operator> inputs = cross.getPlan().getPredecessors(cross);
       
        // currently support 2 inputs
        if (inputs.size() != 2) {
        	throw new LogicalToPhysicalTranslatorException("CROSS currently supports 2 inputs only");
        }
        
        // nested cross not allowed for the moment
        if (cross.isNested()) {
        	throw new LogicalToPhysicalTranslatorException("nested CROSS is not currently supported");
        }

        SOCross soCross = new SOCross(new OperatorKey(scope,nodeGen.getNextNodeId(scope)), cross.getRequestedParallelisam());
        soCross.setAlias(soCross.getAlias());
        soCross.setResultType(DataType.BAG);
        currentPlan.add(soCross);
        logToPhyMap.put(cross, soCross);
        
        for (Operator op : cross.getPlan().getPredecessors(cross)) {
            PactOperator from = logToPhyMap.get(op);
            try {
                currentPlan.connect(from, soCross);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid pact operators in the pact plan when trying to connect SOCross" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
        translateSoftLinks(cross);
    }

    
    @Override
    public void visit(LOForEach foreach) throws FrontendException {
    	
      }
    
    
    /**
     * This function takes in a List of LogicalExpressionPlan and converts them to 
     * a list of PactPlans (to be used in LOForEach translation)
     * 
     * @param plans
     * @return
     * @throws FrontendException 
     */
    private List<PactPlan> translateExpressionPlans(LogicalRelationalOperator loj,
            List<LogicalExpressionPlan> plans ) throws FrontendException {
        List<PactPlan> exprPlans = new ArrayList<PactPlan>();
        if( plans == null || plans.size() == 0 ) {
            return exprPlans;
        }
        
        // Save the current plan onto stack
        currentPlans.push(currentPlan);
        
        for( LogicalExpressionPlan lp : plans ) {
            currentPlan = new PactPlan();
            
            // We spawn a new Dependency Walker and use it 
            // PlanWalker childWalker = currentWalker.spawnChildWalker(lp);
            PlanWalker childWalker = new ReverseDependencyOrderWalkerWOSeenChk(lp);
            
            // Save the old walker and use childWalker as current Walker
            pushWalker(childWalker);
            
            // We create a new ExpToPhyTranslationVisitor to walk the ExpressionPlan
            currentWalker.walk(
                    new ExpToPactTranslationVisitor( 
                            currentWalker.getPlan(), 
                            childWalker, loj, currentPlan, logToPhyMap) );
            
            exprPlans.add(currentPlan);
            popWalker();
        }
        
        // Pop the current plan back out
        currentPlan = currentPlans.pop();

        return exprPlans;
    }
   
    
    
    @Override
    public void visit(LOStore loStore) throws FrontendException {
    	String scope = DEFAULT_SCOPE;
        SOStore store = new SOStore(new OperatorKey(scope, nodeGen.getNextNodeId(scope)));
        store.setAlias(loStore.getAlias());
        store.setSFile(loStore.getOutputSpec());
        store.setInputSpec(loStore.getInputSpec());
        store.setSignature(loStore.getSignature());
        store.setSortInfo(loStore.getSortInfo());
        store.setIsTmpStore(loStore.isTmpStore());
        
        store.setSchema(Util.translateSchema( loStore.getSchema() ));

        currentPlan.add(store);
        
        List<Operator> op = loStore.getPlan().getPredecessors(loStore); 
        PactOperator from;
       
        if(op != null) {
        	
            from = logToPhyMap.get(op.get(0));    
        }        
        else {
            int errCode = 2051;
            String msg = "Did not find a predecessor for Store." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);
        }

        try {
            currentPlan.connect(from, store);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
        logToPhyMap.put(loStore, store);
       
    }
    
    @Override
    public void visit(LOCogroup cg) throws FrontendException {
    	if (cg.getGroupType() != GROUPTYPE.REGULAR) {
        	throw new LogicalToPhysicalTranslatorException("Only REGULAR (Co)GROUP type is currently supported");
    	}
    	
    	String scope = DEFAULT_SCOPE;
    	
    	// if it's a GROUP translate into a Reduce
    	if (cg.getInputs((LogicalPlan)plan).size() == 1) {
    		SOReduce soReduce = new SOReduce(new OperatorKey(scope,nodeGen.getNextNodeId(scope)), cg.getRequestedParallelisam());
	        soReduce.setAlias(cg.getAlias());
	        soReduce.setResultType(DataType.BAG);
	        
	        // for now, only allow Project expressions for GROUP and only grouping by a field
	    	LogicalExpressionPlan lp = cg.getExpressionPlans().get(0).get(0);
	    	if (lp.size() > 1) {
	    		throw new LogicalToPhysicalTranslatorException("Grouping by expressions or tuples is currently not supported");
	    	}
	    	else {
		    	LogicalExpression expr = (LogicalExpression) lp.getOperators().next();
		    	if (!(expr instanceof ProjectExpression)) {
		    		throw new LogicalToPhysicalTranslatorException("Only Project expressions are currently supported in GROUP");
		    	}
		    	else {
		    		soReduce.setFirstKeyPosition(((ProjectExpression)(expr)).getColNum());
		    	}

	    	}
	        
	        currentPlan.add(soReduce);
	        logToPhyMap.put(cg, soReduce);
	        List <Operator> op = cg.getPlan().getPredecessors(cg);
	        PactOperator from = logToPhyMap.get(op.get(0));
	            try {
	                currentPlan.connect(from, soReduce);
	            } catch (PlanException e) {
	                int errCode = 2015;
	                String msg = "Invalid pact operators in the pact plan when trying to connect SOReduce" ;
	                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
	            }
    	}
    	// if it's a CoGROUP translate into a CoGroup
    	else if (cg.getInputs((LogicalPlan)plan).size() == 2) {
    		SOCoGroup soCoGroup = new SOCoGroup(new OperatorKey(scope,nodeGen.getNextNodeId(scope)), cg.getRequestedParallelisam());
    		soCoGroup.setAlias(cg.getAlias());
    		soCoGroup.setResultType(DataType.BAG);
	        currentPlan.add(soCoGroup);
	        logToPhyMap.put(cg, soCoGroup);
	        List<Operator> inputs = cg.getPlan().getPredecessors(cg);
	        for (Operator op : inputs) {
	            PactOperator from = logToPhyMap.get(op);
	        	try {
	                currentPlan.connect(from, soCoGroup);
	            } catch (PlanException e) {
	                int errCode = 2015;
	                String msg = "Invalid pact operators in the pact plan when trying to connect SOcoGroup" ;
	                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
	            }
	        }
    	}
    	// more than 2 inputs are not currently supported
    	else {
    		throw new LogicalToPhysicalTranslatorException("Grouping on more then 2 relations in not currently supported");
    	}
    	
        translateSoftLinks(cg);
    }    
    
	@Override
    public void visit(LOJoin loj) throws FrontendException {
    	String scope = DEFAULT_SCOPE;
        
        // List of join predicates 
        List<Operator> inputs = loj.getPlan().getPredecessors(loj);

        String alias = loj.getAlias();
        
        // currently support 2 inputs
        if (inputs.size() != 2) {
        	throw new LogicalToPhysicalTranslatorException("Joins currently support 2 inputs only");
        }
        
        int[] keyPositions = new int[inputs.size()];
        
        //for each join predicate, find the pactOperators
        for (int i=0;i<inputs.size();i++) {       
            List<LogicalExpressionPlan> plans = (List<LogicalExpressionPlan>)loj.getJoinPlan(i);
            LogicalExpression lExpr = (LogicalExpression) plans.get(0).getOperators().next();
            // retrieve key positions
            keyPositions[i] = ((ProjectExpression)(lExpr)).getColNum();
        }

        //This is the ONLY case to cover
        SOMatch matchOperator = new SOMatch(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), loj.getRequestedParallelisam());
        matchOperator.setAlias(alias);
        matchOperator.setResultType(DataType.BAG);	
        matchOperator.setFirstKeyPosition(keyPositions[0]);
        matchOperator.setSecondKeyPosition(keyPositions[1]);
      
        currentPlan.add(matchOperator);
        logToPhyMap.put(loj, matchOperator);
        
        for (Operator op : inputs) {
            PactOperator from = logToPhyMap.get(op);
            try {
                currentPlan.connect(from, matchOperator);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid pact operators in the pact plan when trying to connect SOMatch" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
        
        translateSoftLinks(loj);
    }
    
    /**
     * updates plan with check for empty bag and if bag is empty to flatten a bag
     * with as many null's as dictated by the schema
     * @param fePlan the plan to update
     * @param joinInput the relation for which the corresponding bag is being checked
     * @throws FrontendException 
     */
    public static void updateWithEmptyBagCheck(PactPlan fePlan, Operator joinInput) throws FrontendException {
        LogicalSchema inputSchema = null;
        try {
            inputSchema = ((LogicalRelationalOperator) joinInput).getSchema();
         
            if(inputSchema == null) {
                int errCode = 1109;
                String msg = "Input (" + ((LogicalRelationalOperator) joinInput).getAlias() + ") " +
                        "on which outer join is desired should have a valid schema";
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.INPUT);
            }
        } catch (FrontendException e) {
            int errCode = 2104;
            String msg = "Error while determining the schema of input";
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
        
        CompilerUtils.addEmptyBagOuterJoin(fePlan, Util.translateSchema(inputSchema));
        
    }
    
    
    private void translateSoftLinks(Operator op) throws FrontendException {
        List<Operator> preds = op.getPlan().getSoftLinkPredecessors(op);

        if (preds == null)
            return;

        for (Operator pred : preds) {
            PactOperator from = logToPhyMap.get(pred);
            currentPlan.createSoftLink(from, logToPhyMap.get(op));
        }
    }
}
