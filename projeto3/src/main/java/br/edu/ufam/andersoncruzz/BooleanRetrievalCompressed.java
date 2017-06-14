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

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

public class BooleanRetrievalCompressed extends Configured implements Tool {
  private MapFile.Reader index, index1, index2, index3, index4;
  private FSDataInputStream collection;
  private Stack<Set<Integer>> stack;

  private BooleanRetrievalCompressed() {}

  private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {
    index = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), fs.getConf());
    index1 = new MapFile.Reader(new Path(indexPath + "/part-r-00001"), fs.getConf());
    index2 = new MapFile.Reader(new Path(indexPath + "/part-r-00002"), fs.getConf());
    index3 = new MapFile.Reader(new Path(indexPath + "/part-r-00003"), fs.getConf());
    index4 = new MapFile.Reader(new Path(indexPath + "/part-r-00004"), fs.getConf());

    collection = fs.open(new Path(collectionPath));
    stack = new Stack<Set<Integer>>();
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();
    try {
    	FileWriter fquery = new FileWriter("consulta.txt", true);
    	fquery.write("Consulta: " + q+"\n\n");
    	int cont = 0;
    	for (Integer i : set) {
    		String line = fetchLine(i);
    		fquery.write(line+"\n\n");
    		System.out.println(i + "\t" + line);
    		cont++;
    	}
    	fquery.write("Total de documentos: "+ cont +"\n\n\n");
    	fquery.close();
    } catch(IOException e){
    	e.printStackTrace();
    }
  }

  private void pushTerm(String term) throws IOException {
	  System.out.println("term: " + term);
	  stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new TreeSet<Integer>();
    System.out.println("fetch Doc: " + term);
    for (PairOfInts pair : fetchPostings(term)) {
      set.add(pair.getLeftElement());
    }

    return set;
  }
//PairOfWritables<VIntWritable, ArrayListWritable<PairOfWritables<VIntWritable, VIntWritable>>>>
  private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {
    Text key = new Text();
    PairOfWritables<VIntWritable, ArrayListWritable<PairOfWritables<VIntWritable, VIntWritable>>> value =
        new PairOfWritables<VIntWritable, ArrayListWritable<PairOfWritables<VIntWritable, VIntWritable>>>();
    //PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> aux = new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();
    ArrayListWritable<PairOfInts> listPairs = new ArrayListWritable<PairOfInts>();
    
    System.out.println("fetch post: " + term);
    key.set(term);
    if(index.get(key, value)!=null);
    else if(index1.get(key, value)!=null);
    else if(index2.get(key, value)!=null);
    else if(index3.get(key, value)!=null);
    else if(index4.get(key, value)!=null);
    
    //Decodificando d-gap
    int dgap = 0;
    
    for(PairOfWritables<VIntWritable, VIntWritable> pairVint : value.getRightElement()){
    	PairOfInts pairint = new PairOfInts();
    	dgap = dgap + pairVint.getLeftElement().get();
    	pairint.set(dgap, pairVint.getRightElement().get());
    	listPairs.add(pairint);
    }
    
    //aux.set(, right);
    return listPairs;
  }

  private String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    return reader.readLine();
  }

  private static final String INDEX = "index";
  private static final String COLLECTION = "collection";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INDEX));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(COLLECTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INDEX) || !cmdline.hasOption(COLLECTION)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(BooleanRetrievalCompressed.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String indexPath = cmdline.getOptionValue(INDEX);
    String collectionPath = cmdline.getOptionValue(COLLECTION);

    if (collectionPath.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      System.exit(-1);
    }

    FileSystem fs = FileSystem.get(new Configuration());

    initialize(indexPath, collectionPath, fs);

    //String[] queries = { "outrageous fortune AND", "white rose AND", "means deceit AND",
      //  "white red OR rose AND pluck AND", "unhappy outrageous OR good your AND OR fortune AND" };
    										//"unhappy OR outrageous OR good AND your AND fortune "
    //String[] queries = {"you OR", "the OR", "yourself AND"};
    //String[] queries = {"are"};
//    String[] queries = {"make happy AND"};
   /* a. outrageous AND fortune
    b. means AND deceit
    c. (white OR red ) AND rose AND pluck
    d. (unhappy OR outrageous OR (good AND your)) AND fortune*/
    				
    String[] queries = {"outrageous fortune AND", //Consulta a 
    					"means deceit AND", //Consulta b
    					"red rose AND pluck AND", //Consulta c
    					"white rose AND pluck AND", //Consulta c
    					"unhappy fortune AND", //consulta d
    					"outrageous fortune AND", //consulta d
    					"good your AND fortune AND", //consulta d
    					"white red OR rose AND pluck AND", //consulta C 
    					"unhappy outrageous OR good your AND OR fortune AND" //consulta D
    					};
    for (String q : queries) {
      System.out.println("Query: " + q);

      runQuery(q);
      System.out.println("");
    }

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}
