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
<p>Questão1. Qual é o tempo de execução da implementação “pairs”? Qual é o tempo de execução da implementação “stripes”? Apresente o tempo médio de 10 execuções de cada uma.</p>
<p><p/>
   <p>Rodada 1: </p>
   <p>Job1: 96.28s</p>
   <p>Job2: 74.46s</p>
   <p>Rodada 2: </p>
   <p>Job1: 95.94s</p>
   <p>Job2: 75.09s</p>
   <p>Rodada 3: </p>
   <p>Job1: 89.86s</p>
   <p>Job2: 68.88s</p>
   <p>Rodada 4: </p>
   <p>Job1: 93.84s</p>
   <p>Job2: 67.72s</p>
   <p>Rodada 5: </p>
   <p>Job1: 93.05s</p>
   <p>Job2: 69.10s</p>
   <p>Rodada 6: </p>
   <p>Job1: 93.82s</p>
   <p>Job2: 68.87s</p>
   <p>Rodada 7: </p>
   <p>Job1: 96.87s</p>
   <p>Job2: 71.57s</p>
   <p>Rodada 8: </p>
   <p>Job1: 96.32s</p>
   <p>Job2: 75.97s</p>
   <p>Rodada 9: </p>
   <p>Job1: 93.63s</p>
   <p>Job2: 68.53s</p>
   <p>Rodada 10: </p>
   <p>Job1: 95.35s</p>
   <p>Job2: 68.64s</p>
   <p>Média dos tempos: 165.379 em segundos</p>

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
   <p>Média dos tempos: 176.483</p>
   
   <p>Questão 3. Quantos pares distintos de PMI foram extraídos?</p>
   <p>R = Foram 154398 pares distintos</p>
   
   
   <p>Questão 4. Qual é o par (x,y) com o PMI mais alto? Justifique intuitivamente este resultado. </p>
	<p>R = ('anjou', 'maine') 3.5900710214945084</p>
    
    
    <p>Questão 5. Quais os três termos que têm o PMI mais alto com “life” e “love”? Quais são os valores do PMI?</p>
    <p>R = ('love', 'dearly') 1.1381069306879603</p>
    <p>('love', 'hermia') 0.8523484281664729</p>
    <p>('love', 'lysander') 0.8010573968734259</p>
    <p>('life', 'save') 1.2311501915646326</p>
    <p>('life', "man's") 0.9369194656085066</p>
    <p>('life', 'death') 0.6945490488475484</p>

