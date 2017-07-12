/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package br.edu.ufam.andersoncruzz;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.WritableUtils;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;

public class Training extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(Training.class);

  private static class MyMapper extends Mapper<LongWritable, Text, Text, ArrayListWritable<PairOfInts>> {
    private static final Text WORD = new Text();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();
    private HashMap <String, ArrayListWritable<PairOfInts>> mapPostings = new HashMap<String, ArrayListWritable<PairOfInts>>();
    private static int flag = -1;
	/*@Override
    public void setup(Context context) throws IOException{
		
	}*/
    
   
    
    @Override
    public void map(LongWritable key, Text doc, Context context)
        throws IOException, InterruptedException {
      String text = doc.toString();
      COUNTS.clear();
      
      //neg#@!=$ pos#@!=$
      String[] docList = text.split("@");
      if (docList[0].equals("pos")) {
    	  COUNTS.increment("pos");
    	  flag = 1;
      }
      else{
    	  COUNTS.increment("neg");
    	  flag = 0;
      }
      
      List<String> terms = Tokenizer.tokenize(docList[1]);
      //PairOfVInt;
      // First build a histogram of the terms.
      for (String term : terms) {
        COUNTS.increment(term);
      }
            
      for (PairOfObjectInt<String> e : COUNTS) {
    	  if(mapPostings.containsKey(e.getLeftElement())){
    		  ArrayListWritable<PairOfInts> postings = mapPostings.get(e.getLeftElement());
    		  postings.add(new PairOfInts(flag, e.getRightElement()));
    		  mapPostings.put(e.getLeftElement(), postings);  
    	  }else {
    		  ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();
    		  postings.add(new PairOfInts(flag, e.getRightElement()));
    		  mapPostings.put(e.getLeftElement(), postings);
    	  }
      }
    }
  
    public void flush(Context context) throws IOException, InterruptedException{
    		String term = "";
    		ArrayListWritable<PairOfInts> post = null;
    		for(Map.Entry<String, ArrayListWritable<PairOfInts>> entry : mapPostings.entrySet()){
    			term = entry.getKey();
    			post = entry.getValue();
    			Collections.sort(post);
    		  
    			WORD.set(term);
    			context.write(WORD, post);
    	  }
      }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
    	flush(context);
    }
  }

   private static class MyReducer extends
       Reducer<Text, ArrayListWritable<PairOfInts>, Text, PairOfWritables<VIntWritable, ArrayListWritable<PairOfInts>>> {
  
  	private final static VIntWritable DF = new VIntWritable();

    @Override
    public void reduce(Text key, Iterable<ArrayListWritable<PairOfInts>> values, Context context)
        throws IOException, InterruptedException {
      Iterator<ArrayListWritable<PairOfInts>> iter = values.iterator();
      ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();
      ArrayListWritable<PairOfInts> aux = new ArrayListWritable<PairOfInts>();

      int df = 0;
      int contPositivo = 1; //Suavizacao de Laplace
      int contNegativo = 1; //Suavizacao de Laplace
      
      PairOfInts positive = new PairOfInts();
      PairOfInts negative = new PairOfInts();
      
      while (iter.hasNext()) {
    	aux = (ArrayListWritable<PairOfInts>) iter.next().clone();
    	for (PairOfInts p : aux){
    		df = df + p.getRightElement();
    		if (p.getLeftElement() == 1){
    			contPositivo = contPositivo + p.getRightElement(); 
    		} else contNegativo = contNegativo + p.getRightElement();
    	}        
        // Merge Lists
    	positive.set(1, contPositivo);
    	negative.set(0, contNegativo);
    	
		postings.add(negative);
		postings.add(positive);
    	//Collections.sort(postings);
      }
            
      DF.set(df);
      context.write(key, new PairOfWritables<VIntWritable, ArrayListWritable<PairOfInts>>(DF, postings));
      
    }
  }

  private Training() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "reducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool name: " + Training.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(Training.class.getSimpleName());
    job.setJarByClass(Training.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ArrayListWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfWritables.class);
    //job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Training(), args);
  }
}
