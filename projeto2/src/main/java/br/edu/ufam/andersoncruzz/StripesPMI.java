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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import br.edu.ufam.andersoncruzz.MapKI.Entry;
import br.edu.ufam.andersoncruzz.MatrixPairs.CounterLinesFile;

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
public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

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
  			MAP.increment(tokens.get(i));
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
	    private static final HMapStIW MAP = new HMapStIW();
	    private static final Text KEY = new Text();

	  
	    @Override
	    public void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {

	    //context.write(key, value);	
	  	String valueTxt = value.toString();
	  	String lines[] = valueTxt.split("\n");
	  			
	  	for (int k=0; k < lines.length; k++) {    		
	  		//List<String> listTerm = tokenizerStripes(lines[k]);
	  		//List<Key_value> listTerm = tokenizerStripes(lines[k]);
	  		Stripe stripe = tokenizerStripes(lines[k]);
	  		MAP.clear();
	  		//MAP.increment(stripe.getKey());
	  		for (int i = 0; i < stripe.getListValue().size(); i++) {
	  			//MAP.increment(listTerm.get(j));
	  	        MAP.put(stripe.getListValue().get(i).key, stripe.getListValue().get(i).value);
	  		}
	  			
	  		KEY.set(stripe.getKey());
	  	    context.write(KEY, MAP);
	  		}
	  	}
  }

  public static Stripe tokenizerStripes (String str) {
  		Stripe stripe = new Stripe();
	  	String[] vectorstr = str.split("\\s+");
		//List <Key_value> listTerm = new ArrayList<Key_value>();
		stripe.setKey(vectorstr[0]);
		
		for (int i=1; i<vectorstr.length; i++) {
			String aux = vectorstr[i];
			if (aux.contains(","))
				aux = aux.replace(",", "");
			if (aux.contains("{"))
				aux = aux.replace("{", "");
			if (aux.contains("}"))
				aux = aux.replace("}", "");
			String[] str_aux = aux.split("=");
			if (str_aux.length >= 2){
				stripe.addKeyValue(str_aux[0], Integer.parseInt(str_aux[1]));
			//Key_value obj = new Key_value(str_aux[0], Integer.parseInt(str_aux[1]));
			//listTerm.add(obj);
			}
		}
  	
		return stripe;
}

  
/*  public static List <Key_value> tokenizerStripes (String str) {
    	String[] vectorstr = str.split("\\s+");
  		List <Key_value> listTerm = new ArrayList<Key_value>();
    	for (int i=1; i<vectorstr.length; i++) {
    		String aux = vectorstr[i];
    		if (aux.contains(","))
    			aux = aux.replace(",", "");
    		if (aux.contains("{"))
    			aux = aux.replace("{", "");
    		if (aux.contains("}"))
    			aux = aux.replace("}", "");
    		String[] str_aux = aux.split("=");
    		Key_value obj = new Key_value(str_aux[0], Integer.parseInt(str_aux[1]));
    		listTerm.add(obj);
    	}
    	
  		return listTerm;
  }

  	public static List <String> tokenizerStripes2 (String str) {
	      	String[] vectorstr = str.split("\\s+");
	    		List <String> listTerm = new ArrayList<String>();
	      	for (int i=1; i<vectorstr.length; i++) {
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
  */
	  private static final class MyPMIReducer extends Reducer<Text, HMapStIW, Text, Text> {
		  
		 private static Map<String, HMapStIW> termTotals = new HashMap<String, HMapStIW>();
		 private static final Text KEY = new Text();
		 private static final Text VALUE = new Text();
		 
		 @Override
		 public void reduce(Text key, Iterable<HMapStIW> values, Context context)
		        throws IOException, InterruptedException {
		      Iterator<HMapStIW> iter = values.iterator();
		      HMapStIW map = new HMapStIW();

		      while (iter.hasNext()) {
		        map.plus(iter.next());
		      }
		      termTotals.put(key.toString(), map);
		      //context.write(key, map);
		    }
		  
			@Override
		    public void cleanup(Context context) throws IOException, InterruptedException{
				List <String> listKey = new ArrayList<String>(termTotals.keySet());
				int numberOflines = context.getConfiguration().getInt("numberOfLines", 0);
				Map<String, Stripe> terms = new HashMap<String, Stripe>();
				
				for (int i=0; i<listKey.size(); i++) {
					String pairstr = listKey.get(i);
					HMapStIW map = termTotals.get(pairstr);
					
					String striper = map.toString();
					terms.put(pairstr, tokenizerStripes(striper));						
				}
				
				for (int i=0; i<terms.size(); i++) {
					String pxkey = listKey.get(i);
					
					Stripe px = terms.get(pxkey);
			  		int probx = 0;
					for (int j = 0; j < px.getListValue().size(); j++) {
						if (px.getListValue().get(j).key.equals(pxkey)) probx = px.getListValue().get(j).value; 
					}
					
					int proby = 0;
					int probxy = 0;
					
					String str = "{";
					for (int j = 0; j < px.getListValue().size(); j++) {
						if (px.getListValue().get(j).key != pxkey ){ 
							probxy = px.getListValue().get(j).value;
							Stripe py = terms.get(px.getListValue().get(j).key);
							for (int s = 0; s < py.getListValue().size(); s++) {
								if (py.getListValue().get(s).key.equals(py.getKey())) proby = py.getListValue().get(s).value; 
							}
							
							//double pmi = Math.log10((probxy/numberOflines)/((probx/numberOflines) * (proby/numberOflines)));
							str = str + py.getKey() + "=" + String.valueOf(probxy) + "-" + String.valueOf(probx) + "-" +  String.valueOf(proby)+ ", ";
						}
			  		}
					
					str = str.substring(0, str.length()-2) + "}";
					KEY.set(pxkey);
					VALUE.set(str);
					context.write(KEY, VALUE);
					
				}
			
			/*	    Iterator<Entry<String>> k = map.entrySet().iterator();
				    while (i.hasNext()) {
				       k.next();
				      HMapStIW key = e.getKey();
				      int value = e.getValue();
					
					
					
					
					for (int j=0; j<values.size(); j++){
						map.;
						if (!pairs[1].equals("*") && termTotals.get(pairstr).doubleValue() >= 10){
						double probabilidadePar = termTotals.get(pairstr).doubleValue()/numberOflines;
						double probabilidadeElemEsq = map.get(pairstr)/numberOflines;
						double probabilidadeElemDir	= termTotals.get(elemDir+",*").doubleValue()/numberOflines;

						double pmi = Math.log10(probabilidadePar/ (probabilidadeElemEsq * probabilidadeElemDir));
						PairOfStrings key = new PairOfStrings(elemEsq, elemDir);
						VALOR.set(pmi);
						context.write(key, VALOR);
					}
				  }*/
				//}
	  				
			}

		  }
	  
	 
	  
  
  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

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
    job.setJobName(StripesPMI.class.getSimpleName());
    job.setJarByClass(StripesPMI.class);

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
    ToolRunner.run(new StripesPMI(), args);
  }
}
