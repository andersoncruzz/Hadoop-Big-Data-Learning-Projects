<p>Aluno PPGI: Anderson Araújo da Cruz</p>
<p>Matrícula: 2170278</p>
<p>email: aac@icomp.ufam.edu.br</p>
<p></p>
<p>Implementação Pairs:</p>
<p>Foram utilizados 2 jobs para calcular o PMI, um job ficou responsável em calcular a frequência relativa do termo e a matriz de co-ocorrência, outro job, ficou responsável em ler a saída do job anterior e calcular o PMI</p>
<p>Descrição job 1:</p>
<ul>
	<p>Job1</p>
	
	class MAPPER:
	method Map (docid a, doc d)
	   linhas = d.getByLines()
	     linhas = linhas.removerPalavrasRepetidasPorLinha()
	       for all linha(x) pertence linhas do
	         Counter.increment //Contador para quantidade de linha
	           for all term w pertence linha(x) do
		     Emit(pair(w, *), 1) //Frequencia por linha
		      for all term u pertence linha(x) do
		 	 Emit(pair(w,u), 1)
	
	class COMBINER
	  method Combiner (pair p, counts[c1,c2,...])
            s = 0
	     for all count c pertence counts[c1,c2,...] do
	       s = s + c
	         Emit(pair p, count s)
	
	<p></p>
	<p></p>
	<p>É necessário um partitioner para garantir que todas as palavras da esquerda vá para o mesmor reduce</p>
	
	  class PARTITIONER (pair p, value v)
	    return p.getLeftElement % numReduce
	
	 class REDUCER
	   method reduce (pair p, counts[c1,c2,...])
	     s = 0
	      for all count c pertence counts[c1,c2,...] do
	        s = s + c
	        Emit(pair p, count s)

</ul>
<p></p>
<p></p>
<p></p>
<p>Configurei no Driver para no Job 2, existir apenas 1 reducer, ou seja, irá ler a saída de todos os mapers, é como um merge da saída anterior</p>
<p>Descrição job 2:</p>
<ul>
	<p>Jobs</p>
	
	class MAPPER:
	  method Map (docid a, doc d)
	    linhas = d.getByLines() 
	     for all linha(x) pertence linhas do
	 	Emit(pair(x), value) //somente repassa o par a chave para o reduce
	
	<p></p>
	<p></p>
	<p></p>
	<li><p></p>
	<p>Como toda a saída do job 1 está no reduce do job 2, usei o padrão in-mapper combining para ir salvando as frequências relativas e co-ocorrências, e somente depois de ter lido toda a saída do map é calculado o PMI no cleanup</p>
	<p></p>
	
	class REDUCER
	   method Setup()
	      termTotals = initialize HashMap()
	   method Reduce(pair [p1,p2,p3], counts[value1,value2,value3])
	     for all pair p and value v do
	       HashMap.add = [p,v]
	   method Cleanup()
	    numberOflines = getCounter //Pegar o Counter do Job 1
	      for all item (pair, value) pertence HashMap do
	        if item.value > 10 do
	           probpair = HashMap(pair).value/numberOflines
	           probpairEsq = HashMap(pairEsq).value/numberOflines
	           probpairDir = HashMap(pairDir).value/numberOflines
	           PMI = Log10(probpair / (probEsq*probDir)) 
	           Emit(pair p, doub PMI)
</ul>

<ul>

  <p>Job 1 </p>
   
   Map - Entrada (LongWritable, Text)
   Map - Saída (PairOfStrings, IntWritable)
   Combiner - Entrada (PairOfStrings, IntWritable)
   Combiner - Saída (PairOfStrings, IntWritable)
   Partitioner - Entrada (PairOfStrings, IntWritable)
   Reducer - Entrada (PairOfStrings, IntWritable)
   Reducer - Saída(PairOfStrings, IntWritable)


</ul>

<ul>

  <p>Job 2 </p>
   
   Map - Entrada (LongWritable, Text)
   Map - Saída (PairOfStrings, IntWritable)
   Reducer - Entrada (PairOfStrings, IntWritable)
   Reducer - Saída(PairOfStrings, IntWritable)


</ul>


<p></p>
<p></p>
<p>Qual é o tempo de execução da implementação “pairs”? Qual é o tempo de execução da implementação “stripes”? Apresente o tempo médio de 10 execuções de cada uma.</p>
<p><p/>
   <p>Rodada 1: </p>
   <p>Job1: 96.28</p>
   <p>Job2: 74.46</p>
   <p>Rodada 2: </p>
   <p>Job1: 95.94</p>
   <p>Job2: 75.09</p>
   <p>Rodada 3: </p>
   <p>Job1: 89.86</p>
   <p>Job2: 68.88</p>
   <p>Rodada 4: </p>
   <p>Job1: 93.84</p>
   <p>Job2: 67.72</p>
   <p>Rodada 5: </p>
   <p>Job1: 93.05</p>
   <p>Job2: 69.10</p>
   <p>Rodada 6: </p>
   <p>Job1: 93.82</p>
   <p>Job2: 68.87</p>
   <p>Rodada 7: </p>
   <p>Job1: 96.87</p>
   <p>Job2: 71.57</p>
   <p>Rodada 8: </p>
   <p>Job1: 96.32</p>
   <p>Job2: 75.97</p>
   <p>Rodada 9: </p>
   <p>Job1: 93.63</p>
   <p>Job2: 68.53</p>
   <p>Rodada 10: </p>
   <p>Job1: 95.35</p>
   <p>Job2: 68.64</p>

<p>Questão 2. Apresente novamente os resultados da Questão 1, mas desta vez com os Combiners desativados.</p>
   <p>Rodada 1: </p>
   <p>Job1: 94.40</p>
   <p>Job2: 68.79</p>
   <p>Rodada 2: </p>
   <p>Job1: 106.14</p>
   <p>Job2: 76.96</p>
   <p>Rodada 3: </p>
   <p>Job1: 91.22</p>
   <p>Job2: 66.85</p>
   <p>Rodada 4: </p>
   <p>Job1: 89.40</p>
   <p>Job2: 70.73</p>
   <p>Rodada 5: </p>
   <p>Job1: 92.43</p>
   <p>Job2: 68.69</p>
   <p>Rodada 6: </p>
   <p>Job1: 92.12</p>
   <p>Job2: 89.18</p>
   <p>Rodada 7: </p>
   <p>Job1: 138.70</p>
   <p>Job2: 95.05</p>
   <p>Rodada 8: </p>
   <p>Job1: 121.00</p>
   <p>Job2: 75.05</p>
   <p>Rodada 9: </p>
   <p>Job1: 95.51</p>
   <p>Job2: 70.56</p>
   <p>Rodada 10: </p>
   <p>Job1: 94.14</p>
   <p>Job2: 67.91</p>
