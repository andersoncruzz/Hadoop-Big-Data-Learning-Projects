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
public class FinalBkpOfMatrixPairs extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(FinalBkpOfMatrixPairs.class);

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
      
      //if (sum >= 10) {
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

  /**
   * Creates an instance of this tool.
   */
  private FinalBkpOfMatrixPairs() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-window", metaVar = "[num]", usage = "cooccurrence window")
    int window = 2;
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

    LOG.info("Tool: " + FinalBkpOfMatrixPairs.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - window: " + args.window);
    LOG.info(" - number of reducers: " + args.numReducers);

    

    
    Job job = Job.getInstance(getConf());
    job.setJobName(FinalBkpOfMatrixPairs.class.getSimpleName());
    job.setJarByClass(FinalBkpOfMatrixPairs.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    job.getConfiguration().setInt("window", args.window);

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
    System.out.println("CONTADOR DE LINHAS: " + jobCntrs.findCounter(CounterLinesFile.numberOfLine).getValue());
    
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new FinalBkpOfMatrixPairs(), args);
  }
}