package org.apache.pig.builtin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.SLoadFunc;
import org.apache.pig.SStoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.stratosphere.executionengine.contractsLayer.PigDataSource;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.CastUtils;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.StorageUtil;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.generic.io.InputFormat;
import eu.stratosphere.pact.common.generic.io.OutputFormat;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;

@SuppressWarnings("unchecked")
public class SPigStorage extends SLoadFunc implements SStoreFuncInterface, LoadPushDown {
	protected RecordReader in = null;
    protected RecordWriter writer = null;
    protected final Log mLog = LogFactory.getLog(getClass());
    protected String signature;
    
    protected PigDataSource pds;
    
    private byte fieldDel = '\t';
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private String loadLocation;

    boolean isSchemaOn = false;
    boolean dontLoadSchema = false;
    protected ResourceSchema schema;
    protected LoadCaster caster;

    private final CommandLine configuredOptions;
    private final Options validOptions = new Options();
    private final static CommandLineParser parser = new GnuParser();

    protected boolean[] mRequiredColumns = null;
    private boolean mRequiredColumnsInitialized = false;

    private void populateValidOptions() {
        validOptions.addOption("schema", false, "Loads / Stores the schema of the relation using a hidden JSON file.");
        validOptions.addOption("noschema", false, "Disable attempting to load data schema from the filesystem.");
    }

    public SPigStorage() {
        this("\t", "");
    }

    /**
     * Constructs a Pig loader that uses specified character as a field delimiter.
     *
     * @param delimiter
     *            the single byte character that is used to separate fields.
     *            ("\t" is the default.)
     * @throws ParseException
     */
    public SPigStorage(String delimiter) {
        this(delimiter, "");
    }

    /**
     * Constructs a Pig loader that uses specified character as a field delimiter.
     * <p>
     * Understands the following options, which can be specified in the second paramter:
     * <ul>
     * <li><code>-schema</code> Loads / Stores the schema of the relation using a hidden JSON file.
     * <li><code>-noschema</code> Ignores a stored schema during loading.
     * </ul>
     * @param delimiter the single byte character that is used to separate fields.
     * @param options a list of options that can be used to modify PigStorage behavior
     * @throws ParseException
     */
    public SPigStorage(String delimiter, String options) {
        populateValidOptions();
        fieldDel = StorageUtil.parseFieldDel(delimiter);
        String[] optsArr = options.split(" ");
        try {
            configuredOptions = parser.parse(validOptions, optsArr);
            isSchemaOn = configuredOptions.hasOption("schema");
            dontLoadSchema = configuredOptions.hasOption("noschema");
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "PigStorage(',', '[options]')", validOptions);
            // We wrap this exception in a Runtime exception so that
            // existing loaders that extend PigStorage don't break
            throw new RuntimeException(e);
        }
    }

	@Override
	public InputFormat getInputFormat() throws IOException {
		return new TextInputFormat();
	}

	//NOTE: Doesn't work
	//Input is received directly from the FileDataSource
	@Override
	public Tuple getNext() throws IOException {
		mProtoTuple = new ArrayList<Object>();
        if (!mRequiredColumnsInitialized) {
            if (signature!=null) {
                Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
                mRequiredColumns = (boolean[])ObjectSerializer.deserialize(p.getProperty(signature));
            }
            mRequiredColumnsInitialized = true;
        }
        try {
            boolean notDone = in.hasNext();
            if (!notDone) {
                return null;
            }
            StringRecord value = (StringRecord)in.next();
            byte[] buf = value.getBytes();
            int len = value.getLength();
            int start = 0;
            int fieldID = 0;
            for (int i = 0; i < len; i++) {
                if (buf[i] == fieldDel) {
                    if (mRequiredColumns==null || (mRequiredColumns.length>fieldID && mRequiredColumns[fieldID]))
                        readField(buf, start, i);
                    start = i + 1;
                    fieldID++;
                }
            }
            // pick up the last field
            if (start <= len && (mRequiredColumns==null || (mRequiredColumns.length>fieldID && mRequiredColumns[fieldID]))) {
                readField(buf, start, len);
            }
            Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);

            return dontLoadSchema ? t : applySchema(t);
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
	}
	
    private void readField(byte[] buf, int start, int end) {
        if (start == end) {
            // NULL value
            mProtoTuple.add(null);
        } else {
            mProtoTuple.add(new DataByteArray(buf, start, end));
        }
    }
    
    private Tuple applySchema(Tuple tup) throws IOException {
        if ( caster == null) {
            caster = getLoadCaster();
        }
        if (signature != null && schema == null) {
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(),
                    new String[] {signature});
            String serializedSchema = p.getProperty(signature+".schema");
            if (serializedSchema == null) return tup;
            try {
                schema = new ResourceSchema(Utils.getSchemaFromString(serializedSchema));
            } catch (ParserException e) {
                mLog.error("Unable to parse serialized schema " + serializedSchema, e);
            }
        }

        if (schema != null) {

            ResourceFieldSchema[] fieldSchemas = schema.getFields();
            int tupleIdx = 0;
            // If some fields have been projected out, the tuple
            // only contains required fields.
            // We walk the requiredColumns array to find required fields,
            // and cast those.
            for (int i = 0; i < fieldSchemas.length; i++) {
                if (mRequiredColumns == null || (mRequiredColumns.length>i && mRequiredColumns[i])) {
                    Object val = null;
                    if(tup.get(tupleIdx) != null){
                        byte[] bytes = ((DataByteArray) tup.get(tupleIdx)).get();
                        val = CastUtils.convertToType(caster, bytes,
                                fieldSchemas[i], fieldSchemas[i].getType());
                    }
                    tup.set(tupleIdx, val);
                    tupleIdx++;
                }
            }
        }
        return tup;
    }


	@Override
	public void prepareToRead(RecordReader reader, InputSplit split)
			throws IOException {
		this.in = reader;

	}

	@Override
	public void setLocation(String location, PigDataSource pds) throws IOException {
		loadLocation = location;
		this.pds = pds;
		
		//Write the input file path in the Global Configuration
		//so that the TextInputFormat can read from it
		GlobalConfiguration.loadConfiguration("/home/hinata/workspace/mystrato_0.2/stratosphere/stratosphere-dist/target/stratosphere-dist-0.2-bin/stratosphere-0.2/conf");
		Configuration config = GlobalConfiguration.getConfiguration();
		
		config.setString("pact.input.file.path", location);
		
		//merge the config object into the GlobalConfiguration
		GlobalConfiguration.includeConfiguration(config);
		
	}
	
	@Override
    public PigDataSource getDataSource(){
		return this.pds;
	}
	
	@Override
    public void setDataSource(PigDataSource dataSrc){
		this.pds = dataSrc;
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
	}

	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return new RecordOutputFormat();
	}

	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;

	}

	@Override
	public void putNext(Tuple t) throws IOException {
		  try {
			  PactRecord pr = new PactRecord(t.size());
			  //copy the tuple contents in the PactRecord
			  for(int i=0; i< t.size(); i++)
				  pr.setField(i, (Value) t.get(i));
			  writer.emit(pr);
	        } catch (InterruptedException e) {
	            throw new IOException(e);
	        }	
        }

	@Override
	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		return SLoadFunc.getAbsolutePath(location, curDir);
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
		this.signature = signature;
	}

	@Override
	public void setStoreLocation(String location) throws IOException {
		GlobalConfiguration.loadConfiguration("/home/hinata/thesis/stratosphere-dev/stratosphere/stratosphere-dist/target/stratosphere-dist-0.2-bin/stratosphere-0.2/conf/");
		Configuration config = GlobalConfiguration.getConfiguration();
		config.setString("OutputLocation", location);
		//merge the config object into the GlobalConfiguration
		GlobalConfiguration.includeConfiguration(config);
	}

	@Override
	public List<OperatorSet> getFeatures() {
		return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
	}

	@Override
	public RequiredFieldResponse pushProjection(
			RequiredFieldList requiredFieldList) throws FrontendException {
		if (requiredFieldList == null)
            return null;
        if (requiredFieldList.getFields() != null)
        {
            int lastColumn = -1;
            for (RequiredField rf: requiredFieldList.getFields())
            {
                if (rf.getIndex()>lastColumn)
                {
                    lastColumn = rf.getIndex();
                }
            }
            mRequiredColumns = new boolean[lastColumn+1];
            for (RequiredField rf: requiredFieldList.getFields())
            {
                if (rf.getIndex()!=-1)
                    mRequiredColumns[rf.getIndex()] = true;
            }
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            try {
                p.setProperty(signature, ObjectSerializer.serialize(mRequiredColumns));
            } catch (Exception e) {
                throw new RuntimeException("Cannot serialize mRequiredColumns");
            }
        }
        return new RequiredFieldResponse(true);
	}

}
