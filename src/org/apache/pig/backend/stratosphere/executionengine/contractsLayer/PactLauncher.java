package org.apache.pig.backend.stratosphere.executionengine.contractsLayer;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.tools.pigstats.PigStats;

public class PactLauncher {
	
	public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
    
    private static final Log log = LogFactory.getLog(PactLauncher.class);
    private static final String USER_DIR = System.getProperty("user.dir");

	public PigStats launchPig(PactPlan pp, String jobName, PigContext pc) throws IOException {  
		
		Properties properties = new Properties();
        PropertiesUtil.loadDefaultProperties(properties); 
		PropertiesUtil.storePactPlan(properties.getProperty("pig.pactplan.filepath"), pp);
		PropertiesUtil.storePigContext(properties.getProperty("pig.context.filepath"), pc);
			
		//create the jar if it doesn't exist already
		File jarFile = new File("pigpact.jar");		
		if (!(jarFile.exists())){
			jarFile = createProgramJar("pigpact.jar", "build/classes/org/apache/pig/");
		}

  		/*String[] programArgs = null;
  		
  		try {  			
  			//create the PactProgram
			PactProgram pactProg = new PactProgram(jarFile, programArgs);
			String stratoBin = properties.getProperty("pig.stratosphere.path"); 
			String confPath = stratoBin.substring(0, stratoBin.length()-4) + "conf/";
			System.out.println("Conf Path: " +confPath);
			GlobalConfiguration.loadConfiguration(confPath);
			Configuration config = GlobalConfiguration.getConfiguration();
			//create the pact client
			Client client = new Client(config);
			
			//run the pact program
			client.run(pactProg);
  		} catch (ProgramInvocationException e) {
  			log.error(e);
  		} catch (ErrorInPlanAssemblerException e) {
  			log.error(e);
  		}*/
  		
  		String s = null;
  		
  		String command = properties.getProperty("pig.stratosphere.path") + File.separator + "pact-client.sh run -v -w -j " 
  		+ USER_DIR + File.separator +"pigpact.jar";
  		Process p = Runtime.getRuntime().exec(command);
  		
  		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

        // read the output from the command
        while ((s = stdInput.readLine()) != null) {
            System.out.println(s);
        }
        
        // read any errors from the attempted command
        while ((s = stdError.readLine()) != null) {
            System.out.println(s);
        }
  		
        log.info( "100% complete");

        //no stats for the moment
        return null;
  
	}

	
	/**
	 * Creates a jar and returns it
	   It is constructing and executing the corresponding shell command
	 * @param jarName: name of the jar to be created
	 * @param classes: paths of classes to be included in the jar
	 * @return File
	 * @throws IOException
	 */
	private static File createProgramJar(String jarName, String classes) throws IOException{
		try {
			//Update the manifest
			Manifest manifest = new Manifest();
			manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
			manifest.getMainAttributes().putValue("Pact-Assembler-Class", "PigPlanAssemblerExt");
			manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, classes);
			File assemblerClass = new File("PigPlanAssemblerExt.class");
			JarOutputStream target = new JarOutputStream(new FileOutputStream(jarName), manifest);
			add(new File(classes), target);
			add(assemblerClass, target);
			target.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

		File pactJar = new File(jarName);
		return pactJar;
	}

	private static void add(File source, JarOutputStream target) throws IOException
	{
	  BufferedInputStream in = null;
	  try
	  {
		  // skip unecessary folders
		if (source.getPath().contains("test") || source.getPath().contains("tutorial") || source.getPath().contains("pigunit"))
			return;
	    if (source.isDirectory()) {
	      String name = source.getPath().replace("\\", "/").replace("build/classes/", "");
	      if (!name.isEmpty())
	      {
	        if (!name.endsWith("/"))
	          name += "/";
	        JarEntry entry = new JarEntry(name);
	        entry.setTime(source.lastModified());
	        target.putNextEntry(entry);
	        target.closeEntry();
	      }
	      for (File nestedFile: source.listFiles())
	        add(nestedFile, target);
	      return;
	    }

	    JarEntry entry = new JarEntry(source.getPath().replace("\\", "/").replace("build/classes/", ""));
	    entry.setTime(source.lastModified());
	    target.putNextEntry(entry);
	    in = new BufferedInputStream(new FileInputStream(source));

	    byte[] buffer = new byte[1024];
	    while (true)
	    {
	      int count = in.read(buffer);
	      if (count == -1)
	        break;
	      target.write(buffer, 0, count);
	    }
	    target.closeEntry();
	  }
	  finally
	  {
	    if (in != null)
	      in.close();
	  }
	}

	public void reset() {
		Properties properties = new Properties();
        PropertiesUtil.loadDefaultProperties(properties); 
        new File(properties.getProperty("pig.context.filepath")).delete();
        new File(properties.getProperty("pig.pactplan.filepath")).delete();
	}
	

}
