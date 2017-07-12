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

import it.unimi.dsi.fastutil.ints.Int2FloatFunction;

import java.awt.geom.Arc2D.Float;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.digester.ParserFeatureSetterFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ch.qos.logback.core.pattern.parser.Parser;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;


public class Classification extends Configured implements Tool {

	public class Probabilites {
	    private String key;
	    private int DF;
	    private int positive;
	    private int negative;
	    public Probabilites(String key, int DF, int positive, int negative)
	    {
	        this.key   = key;
	        this.DF = DF;
	        this.positive = positive;
	        this.negative = negative;
	    }
	    public Probabilites(String key)
	    {
	        this.key = key;
	        this.DF = 0;
	        this.positive = 0;
	        this.negative = 0;
	    }

	    public void setKey(String key)   { this.key = key; }
	    public void setDF(int DF) { this.DF = DF; }
	    public void setPositive(int positive) { this.positive = positive; }
	    public void setNegative(int negative) { this.negative = negative; }
	    	    
	    public String getKey()   { return this.key; }
	    public int getDF() { return this.DF; }
	    public int getPositive() { return this.positive; }
	    public int getNegative() { return this.negative; }

	
	}	
	
 private MapFile.Reader index, index1, index2, index3, index4;  
 // private FSDataInputStream collection;
  private ArrayList<Probabilites> listProbabilites;
  private int contAcertouMatrixPositivo = 0;
  private int contAcertouMatrixNegativo = 0;
  private int contErrouMatrixPositivo = 0;
  private int contErrouMatrixNegativo = 0;

  private Classification() {}

  private void initialize(String indexPath, FileSystem fs) throws IOException {
    index = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), fs.getConf());
    index1 = new MapFile.Reader(new Path(indexPath + "/part-r-00001"), fs.getConf());
    index2 = new MapFile.Reader(new Path(indexPath + "/part-r-00002"), fs.getConf());
    index3 = new MapFile.Reader(new Path(indexPath + "/part-r-00003"), fs.getConf());
    index4 = new MapFile.Reader(new Path(indexPath + "/part-r-00004"), fs.getConf());
    
   // collection = fs.open(new Path(collectionPath));
    //stack = new Stack<Set<Integer>>();
    listProbabilites = new ArrayList<Probabilites>();
  }

  private void runCollectProbabilites(String instance) throws IOException {
    String[] attributes = instance.split("\\s+");

    for (String attr : attributes) {
        pushTerm(attr);
    }

   // Set<Integer> set = stack.pop();

    /*for (Probabilites p : listProbabilites) {
      System.out.println("Term: " + p.getKey() + " Pos: " + p.getPositive() + " Neg: " + p.getNegative() + " DF: " + p.getDF());
    }*/
  }

  private void runClassification(String label) throws IOException{
	int aprioriPositiveFrequencia = 0;
	int aprioriNegativeFrequencia = 0;
	for (Probabilites p : listProbabilites) {
		if (p.getKey().equals("pos")) aprioriPositiveFrequencia = p.getPositive();
		else if (p.getKey().equals("neg")) aprioriNegativeFrequencia = p.getNegative();
		
		if (aprioriPositiveFrequencia != 0 && aprioriNegativeFrequencia != 0) break;
		//System.out.println("Term: " + p.getKey() + " Pos: " + p.getPositive() + " Neg: " + p.getNegative() + " DF: " + p.getDF());
    }
	//double classificationPos = Math.log10((double) positiveProbabilites/((double)positiveProbabilites + negativeProbabilites));
	//double classificationNeg = Math.log10((double) negativeProbabilites/((double)positiveProbabilites + negativeProbabilites));
	//double aprioriProbabilitesPos = (double) aprioriPositiveFrequencia/((double)aprioriPositiveFrequencia + aprioriNegativeFrequencia);
	//double aprioriProbabilitesNeg = (double) aprioriNegativeFrequencia/((double)aprioriPositiveFrequencia + aprioriNegativeFrequencia);

	double aprioriProbabilitesPos = Math.log10((double) aprioriPositiveFrequencia/((double)aprioriPositiveFrequencia + aprioriNegativeFrequencia));
	double aprioriProbabilitesNeg = Math.log10((double) aprioriNegativeFrequencia/((double)aprioriPositiveFrequencia + aprioriNegativeFrequencia));


	double aposterioriProbabilitesPos = aprioriProbabilitesPos;
	double aposterioriProbabilitesNeg = aprioriProbabilitesNeg;
	
	for (Probabilites p : listProbabilites){
		System.out.println("Termo: " + p.getKey());
		if (!p.getKey().equals("pos") && !p.getKey().equals("neg")){
			System.out.println(" Pos: " + p.getPositive() + " Neg: " + p.getNegative() + " DF: " + p.getDF());
			System.out.println("aposterioriPos: " + aposterioriProbabilitesPos );
			System.out.println("aposterioriNeg: " + aposterioriProbabilitesNeg );			
			//classificationPos = classificationPos + Math.log10((p.getPositive()/(double)positiveProbabilites));
			//classificationNeg = classificationNeg + Math.log10((p.getNegative()/(double)negativeProbabilites));
			//aposterioriProbabilitesPos = aposterioriProbabilitesPos * (p.getPositive()/(double)aprioriPositiveFrequencia);
			//aposterioriProbabilitesNeg = aposterioriProbabilitesNeg * (p.getNegative()/(double)aprioriNegativeFrequencia);

			aposterioriProbabilitesPos = aposterioriProbabilitesPos + Math.log10((p.getPositive()/(double)aprioriPositiveFrequencia));
			aposterioriProbabilitesNeg = aposterioriProbabilitesNeg + Math.log10((p.getNegative()/(double)aprioriNegativeFrequencia));

			
			//System.out.println ("positive/posFrequencia" + ((p.getPositive()/(double)aprioriPositiveFrequencia)));
			//System.out.println ("negative/negFrequencia" + ((p.getNegative()/(double)aprioriNegativeFrequencia)));
		}
	}
  System.out.println("\naprioriProbPos: " + aprioriProbabilitesPos + " aprioriProbNeg: " + aprioriProbabilitesNeg);
  System.out.println("\naposterioriPos: " + aposterioriProbabilitesPos + " aposterioriNeg: " + aposterioriProbabilitesNeg);
  
  double probabilitesPos = aposterioriProbabilitesPos/((double)aposterioriProbabilitesPos + aposterioriProbabilitesNeg);
  double probabilitesNeg = aposterioriProbabilitesNeg/((double)aposterioriProbabilitesPos + aposterioriProbabilitesNeg);
  
  System.out.println("probabilitesPos: " + probabilitesPos + " probabilitesNeg: " + probabilitesNeg);
  
  String classe = ""; 
  if (aposterioriProbabilitesPos > aposterioriProbabilitesNeg) {
	  System.out.println("CLASSIFICACAO POSITIVA COM " + probabilitesNeg*100 + "%");
	  classe = "pos";
  } else { 
	  System.out.println("CLASSIFICACAO NEGATIVA COM " + probabilitesPos*100 + "%");
	  classe = "neg";
  }
  
  if (classe.equals(label) && label.equals("pos")) {
	  contAcertouMatrixPositivo++;
  } 

  if (!classe.equals(label) && label.equals("pos")) {
	  contErrouMatrixPositivo++;
  }  
  
  if (classe.equals(label) && label.equals("neg")) {
	  contAcertouMatrixNegativo++;
  }

  if (!classe.equals(label) && label.equals("neg")) {
	  contErrouMatrixNegativo++;
  }
	  

  }
  private void pushTerm(String attr) throws IOException {
	 listProbabilites.add(fetchDocumentSet(attr));
  }

  private Probabilites fetchDocumentSet(String term) throws IOException {
    Probabilites set = new Probabilites(term);
    //System.out.println("term: " + term);
    for (PairOfInts pair : fetchPostings(term)) {
    	//System.out.println("Termo: " + term + " getLeft: " + pair.getLeftElement() + " getRight" + pair.getRightElement());
    	if (pair.getLeftElement() == 0)
    		set.setNegative((pair.getRightElement()));
    	else if	(pair.getLeftElement() == 1)
    		set.setPositive((pair.getRightElement()));
    }
    set.setDF(set.getPositive() + set.getNegative());
    
    return set;
  }

  private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {
    Text key = new Text();
    PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value =
        new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();
    
    ArrayListWritable<PairOfInts> lis = new ArrayListWritable<PairOfInts>();
    lis.add(new PairOfInts(0,1));
    lis.add(new PairOfInts(1,1));
    value.set(new IntWritable(2), lis);
    
    key.set(term);
    if(index.get(key, value)!=null);
    else if(index1.get(key, value)!=null);
    else if(index2.get(key, value)!=null);
    else if(index3.get(key, value)!=null);
    else if(index4.get(key, value)!=null);


    return value.getRightElement();
  }


  private static final String INDEX = "index";
  private static final String FILE = "file";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path index").create(INDEX));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
            .withDescription("input path training").create(FILE));

    
    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INDEX) || !cmdline.hasOption(FILE)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(Classification.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String indexPath = cmdline.getOptionValue(INDEX);
    String filePath = cmdline.getOptionValue(FILE);
    FileSystem fs = FileSystem.get(new Configuration());

    initialize(indexPath, fs);
    
   // String aux = "an american werewolf in london is john landis' groundbreaking feature about an american tourist who gets himself bitten by a werewolf in jolly old england .  the groundbreaking part of the movie is the special effects .  more specifically , the makeup used for the transformation of a man into a werewolf ; and for the ghosts that haunt the main character .  even twenty years after its release , that part of the movie is still impressive .  although , i would have to say that it really is the only part of the movie that could be considered impressive .  the rest of the movie is a run of the mill werewolf flick with some extra gore thrown in for good measure .  if it weren't for the cutting edge makeup effects used in the werewolf transformation it is most likely that this is a film that would have gone largely unnoticed when it was released back in 1980 .  and with good reason -- the acting isn't great and neither is the writing .  well ok , we don't actually expect either of those things to be great in a horror film .  but one other important element is lacking here too -- it isn't scary .  with no exception , you know what is going to happen before it happen";
    //aux = "pos neg " + aux;
    //String[] queries = aux.split(" ");
    try(BufferedReader br = new BufferedReader(new FileReader(filePath))) {
        int cont = 1;
    	for(String line; (line = br.readLine()) != null; ) {
    		String[] obj = line.split("@");
    		String instanceTest = "pos neg " + obj[1];
    		List<String> terms = Tokenizer.tokenize(instanceTest);
        	for (String q : terms) {
        		//System.out.println("Query: " + q);

        		runCollectProbabilites(q);
        		//System.out.println("");
        	}
        	System.out.println("instance: " + cont + " label: " + obj[0]);
        	cont++;
        	runClassification(obj[0]);
        	System.out.println("\n\n");
        	listProbabilites = new ArrayList<Probabilites>();
    	}
        
    }
    
    System.out.println("\n---------------");
    System.out.println("Matrix Confusion");
    System.out.println("acertou positivo = " + contAcertouMatrixPositivo);
    System.out.println("errou positivo = " + contErrouMatrixPositivo + "\n");
    System.out.println("acertou negativo = " + contAcertouMatrixNegativo);
    System.out.println("errou negativo = " + contErrouMatrixNegativo);
    
    float acuracia = 100*(1-((contErrouMatrixNegativo + contErrouMatrixPositivo)/
    		(float)(contErrouMatrixNegativo + contErrouMatrixPositivo + contAcertouMatrixNegativo + contAcertouMatrixPositivo)));
    System.out.println("\nacuracia = " + acuracia + "%");
    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Classification(), args);
  }
}
