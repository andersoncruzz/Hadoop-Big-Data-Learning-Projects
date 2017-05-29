/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package br.edu.ufam.andersoncruzz;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import br.edu.ufam.andersoncruzz.MatrixPairs.CounterLinesFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * <p>
 * Implementation of the "pairs" algorithm for computing co-occurrence matrices from a large text
 * collection. This algorithm is described in Chapter 3 of "Data-Intensive Text Processing with 
 * MapReduce" by Lin &amp; Dyer, as well as the following paper:
 * </p>
 *
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the Masses: A Case Study in
 * Computing Word Co-occurrence Matrices with MapReduce.</b> <i>Proceedings of the 2008 Conference
 * on Empirical Methods in Natural Language Processing (EMNLP 2008)</i>, pages 419-428.</blockquote>
 *
 * @author Jimmy Lin
 */
public class MatrixStripes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(MatrixStripes.class);

  public static enum CounterLinesFile {
	  	numberOfLine
	  }; 

  
  private static final class MyMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
    private static final HMapStIW MAP = new HMapStIW();
    private static final Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

  	String valueTxt = value.toString();
  	String lines[] = valueTxt.split("\n");
  			
  	for (int k=0; k < lines.length; k++) {

  		List<String> tokens = Tokenizer.tokenize(lines[k]);    		
  		
  		if (tokens.size() > 1) context.getCounter(CounterLinesFile.numberOfLine).increment(1);
  		for (int i = 0; i < tokens.size(); i++) {
  			MAP.clear();
  			for (int j = 0; j < tokens.size(); j++) {
  	          if (!tokens.get(i).equals(tokens.get(j)))
  				MAP.increment(tokens.get(j));
  			}
  			
  			KEY.set(tokens.get(i));
  	        context.write(KEY, MAP);
  			}
  		}
  	}
  }

  private static final class MyReducer extends Reducer<Text, HMapStIW, Text, HMapStIW> {
    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  private static final class MyPMIMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
	//    private static final Text VALUE = new Text();
	  //  private static final Text KEY = new Text();

	    @Override
	    public void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {

	    //context.write(key, value);	
	  	String valueTxt = value.toString();
	  	String lines[] = valueTxt.split("\n");
	  			
	  	for (int k=0; k < lines.length; k++) {    		
	  		List<String> listTerm = tokenizerStripes(lines[k]);
	  		
	  		for (int i = 0; i < listTerm.size(); i++) {
	  			MAP.clear();
	  			for (int j = 0; j < listTerm.size(); j++) {
	  	          if (!tokens.get(i).equals(tokens.get(j)))
	  				MAP.increment(tokens.get(j));
	  			}
	  			
	  			KEY.set(tokens.get(i));
	  	        context.write(KEY, MAP);
	  			}
	  		}

	  		
	  		//context.write(KEY, VALUE);
	  		}
	 
	  	}
	    public List <String> tokenizerStripes (String str) {
	      	String[] vectorstr = str.split("\\s+");
	    		List <String> listTerm = new ArrayList<String>();
	      	for (int i=0; i<vectorstr.length; i++) {
	      		String aux = vectorstr[i];
	      		if (aux.contains(","))
	      			aux = aux.replace(",", "");
	      		if (aux.contains("{"))
	      			aux = aux.replace("{", "");
	      		if (aux.contains("}"))
	      			aux = aux.replace("}", "");
	      		listTerm.add(aux);
	      	}
	      	
	    		return listTerm;
	    	}

	    
  }
  
	  private static final class MyPMIReducer extends Reducer<Text, HMapStIW, Text, HMapStIW> {
		  
		  @Override
		 public void reduce(Text key, Iterable<HMapStIW> values, Context context)
		        throws IOException, InterruptedException {
		      Iterator<HMapStIW> iter = values.iterator();
		      HMapStIW map = new HMapStIW();

		      while (iter.hasNext()) {
		        map.plus(iter.next());
		      }

		      context.write(key, map);
		    }
	  }	  

  
  /**
   * Creates an instance of this tool.
   */
  private MatrixStripes() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + MatrixPairs.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(MatrixStripes.class.getSimpleName());
    job.setJarByClass(MatrixStripes.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(HMapStIW.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(HMapStIW.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    Counters jobCntrs = job.getCounters();
    int numberOflines = (int) jobCntrs.findCounter(CounterLinesFile.numberOfLine).getValue();
    System.out.println("CONTADOR DE LINHAS: " + numberOflines);


    System.out.println("OUTRO JOB EM EXECUÇÃO: ");
    
    Job job2 = Job.getInstance(getConf());
    job2.setJobName("Calculando PMI");
    job2.setJarByClass(MatrixPairs.class);

    job2.getConfiguration().setInt("numberOfLines", numberOflines);

    job2.setNumReduceTasks(1);

    FileInputFormat.addInputPaths(job2, args.output);
    FileOutputFormat.setOutputPath(job2, new Path(args.output+"/PMI"));
    
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(HMapStIW.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(HMapStIW.class);

    job2.setMapperClass(MyPMIMapper.class);
    job2.setReducerClass(MyPMIReducer.class);
    
    startTime = System.currentTimeMillis();
    job2.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");    
    
    
    
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new MatrixStripes(), args);
  }
}
