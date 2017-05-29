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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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


public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  public static enum CounterLinesFile {
  	numberOfLine
  }; 
  
  
  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private static final PairOfStrings PAIR = new PairOfStrings();
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
     
    	String valueTxt = value.toString();
    	String lines[] = valueTxt.split("\n");
    			
    	for (int k=0; k < lines.length; k++) {

    		List<String> tokens = Tokenizer.tokenize(lines[k]);    		
    		
    		if (tokens.size() > 1) context.getCounter(CounterLinesFile.numberOfLine).increment(1);
    		for (int i = 0; i < tokens.size(); i++) {
    			PAIR.set(tokens.get(i), "*");
    			context.write(PAIR, ONE);
    			for (int j = i+1; j < tokens.size(); j++) {
    				if (!tokens.get(i).equals(tokens.get(j))) {
    					PAIR.set(tokens.get(i), tokens.get(j));
    					context.write(PAIR, ONE);
    					PAIR.set(tokens.get(j), tokens.get(i));
    					context.write(PAIR, ONE);
    				}
    			}
    		}
    	}
    }
  }

 private static final class MyCombiner extends
  Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
private static final IntWritable SUM = new IntWritable();

@Override
public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
  Iterator<IntWritable> iter = values.iterator();
  int sum = 0;
  while (iter.hasNext()) {
    sum += iter.next().get();
  }
  
	  SUM.set(sum);
	  context.write(key, SUM);
 
  }
}

  
  private static final class MyReducer extends
      Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      
      SUM.set(sum);
      context.write(key, SUM);
      	
      }
  }

  private static final class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {
    @Override
    public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }


  
  private static final class MyPMIMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
	    private static final PairOfStrings PAIR = new PairOfStrings();
	    private static final IntWritable VALUE = new IntWritable(1);	      	    
	    
	    @Override
	    public void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {
	     
	    	String valueTxt = value.toString();
	    	String lines[] = valueTxt.split("\n");
	    			
	    	for (int k=0; k < lines.length; k++) {
	    		String[] parts = lines[k].split("\\s+");
	    		PAIR.set(parts[0].substring(1, parts[0].indexOf(",")), parts[1].replace(")", ""));
	    		VALUE.set(Integer.parseInt(parts[2]));
	    		context.write(PAIR, VALUE);
	    	}
	  }
  }
  	  
	  private static final class MyPMIReducer extends
	      Reducer<PairOfStrings, IntWritable, PairOfStrings, DoubleWritable> {
	    private static final DoubleWritable VALOR = new DoubleWritable(1.0);
	    private static Map<String, Integer> termTotals = new HashMap<String, Integer>();
	    
		@Override
	    public void setup(Context context) throws IOException{
			
		}
	        
	    @Override
	    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
	        throws IOException, InterruptedException {
	    	Iterator<IntWritable> iter = values.iterator();
	    	int value = 0;
	    	while (iter.hasNext()) {
	    	    value += iter.next().get();
	    	}
	    	  
	    	termTotals.put(key.getLeftElement()+","+key.getRightElement(), value);
	      }
	    
		@Override
	    public void cleanup(Context context) throws IOException, InterruptedException{
			List <String> listKey = new ArrayList<String>(termTotals.keySet());
			int numberOflines = context.getConfiguration().getInt("numberOfLines", 0);
			
			for (int i=0; i<listKey.size(); i++) {
				String pairstr = listKey.get(i);
				String[] pairs = pairstr.split(",");
				String elemEsq = pairs[0];
				String elemDir = pairs[1];
			
				if (!pairs[1].equals("*") && termTotals.get(pairstr).doubleValue() >= 10){
					double probabilidadePar = termTotals.get(pairstr).doubleValue()/numberOflines;
					double probabilidadeElemEsq = termTotals.get(elemEsq+",*").doubleValue()/numberOflines;
					double probabilidadeElemDir	= termTotals.get(elemDir+",*").doubleValue()/numberOflines;

					double pmi = Math.log10(probabilidadePar/ (probabilidadeElemEsq * probabilidadeElemDir));
					PairOfStrings key = new PairOfStrings(elemEsq, elemDir);
					VALOR.set(pmi);
					context.write(key, VALOR);

				}
			}
			
		}

	  }
 
  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 5;

  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName("Calculando matriz co-ocorrencia, frequencias e numero de linhas do arquivo");
    job.setJarByClass(PairsPMI.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStrings.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    Counters jobCntrs = job.getCounters();
    int numberOflines = (int) jobCntrs.findCounter(CounterLinesFile.numberOfLine).getValue();
    System.out.println("CONTADOR DE LINHAS: " + numberOflines);
    System.out.println("OUTRO JOB EM EXECUÇÃO: ");
 
    Job job2 = Job.getInstance(getConf());
    job2.setJobName("Calculando PMI");
    job2.setJarByClass(PairsPMI.class);

    job2.getConfiguration().setInt("numberOfLines", numberOflines);

    job2.setNumReduceTasks(1);

    FileInputFormat.addInputPaths(job2, args.output);
    FileOutputFormat.setOutputPath(job2, new Path(args.output+"/PMI"));
    
    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(DoubleWritable.class);

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
    ToolRunner.run(new PairsPMI(), args);
  }
}

