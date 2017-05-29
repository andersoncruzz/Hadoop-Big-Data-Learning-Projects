<p>Aluno PPGI: Anderson Araújo da Cruz</p>
<p>Matrícula: 2170278</p>
<p>email: aac@icomp.ufam.edu.br</p>
<p></p>
<p>Implementação Pairs:</p>
<p>Foram utilizados 2 jobs para calcular o PMI, um job ficou responsável em calcular a frequência relativa do termo e a matriz de co-ocorrência, outro job, ficou responsável em ler a saída do job anterior e calcular o PMI</p>
<p>Descrição job 1:</p>
<ul>
	<p>Map</p>
	
	<li><p>class MAPPER:</p>
	<p>method Map (docid a, doc d)</p>
	<p> linhas = d.getByLines() </p>
	<p> linhas = linhas.removerPalavrasRepetidasPorLinha() </p>
	<p>   for all linha(x) pertence linhas do</p>
	<p>    Counter.increment //Contador para quantidade de linha</p>
	<p>    for all term w pertence linha(x) do</p>
	<p>	Emit(pair(w, *), 1) //Frequencia por linha</p>
	<p>	    for all term u pertence linha(x) do</p>
	<p>	 	Emit(pair(w,u), 1)</p>
	<p></p></li>
	<p></p>
	
	<p>class COMBINER</p>
	<p>method Combiner (pair p, counts[c1,c2,...])</p>
	<p>  s = 0</p>
	<p>    for all count c pertence counts[c1,c2,...] do</p>
	<p>      s = s + c </p>
	<p>    Emit(pair p, count s)</p>	
	<p></p>
	<p></p>
	<li><p>É necessário um partitioner para garantir que todas as palavras da esquerda vá para o mesmor reduce</p>
	
	<p>class PARTITIONER (pair p, value v)</p>
	<p>   return p.getLeftElement % numReduce </p>
	<p></p></li>
	<p></p>
	
	<li><p>class REDUCER</p>
	<p>method reduce (pair p, counts[c1,c2,...])</p>
	<p>  s = 0</p>
	<p>    for all count c pertence counts[c1,c2,...] do</p>
	<p>      s = s + c </p>
	<p>    Emit(pair p, count s)</p></li>

</ul>
<p></p>
<p></p>
<p></p>
<p>Configurei no Driver para no Job 2, existir apenas 1 reducer, ou seja, irá ler a saída de todos os mapers, é como um merge da saída anterior</p>
<p>Descrição job 2:</p>
<ul>
	<li><p>class MAPPER:</p>
	<p>method Map (docid a, doc d)</p>
	<p> linhas = d.getByLines() </p>
	<p>   for all linha(x) pertence linhas do</p>
	<p>	Emit(pair(x), value) //somente repassa o par a chave para o reduce</p>
	
	<p></p></li>
	<li><p></p>
	<p>Como toda a saída do job 1 está no reduce do job 2, usei o padrão in-mapper combining para ir salvando as frequências relativas e co-ocorrências, e somente depois de ter lido toda a saída do map é calculado o PMI no cleanup</p>
	<p></p>
	
	<p>class REDUCER</p>
	<p>  method Setup()</p>
	<p>    termTotals = initialize HashMap()</p>
	<p></p>
	<p> method Reduce(pair [p1,p2,p3], counts[value1,value2,value3])</p>
	<p>    for all pair p do</p>
	<p>      HashMap.add = [p,value] </p>
	<p> </p>
	<p> method Cleanup()</p>
	<p>   numberOflines = getCounter //Pegar o Counter do Job 1</p>
	<p>     for all item (pair, value) pertence HashMap do
	<p>        if item.value > 10 do
	<p>           probpair = HashMap(pair).value/numberOflines<p>
	<p>           probpairEsq = HashMap(pairEsq).value/numberOflines<p>
	<p>           probpairDir = HashMap(pairDir).value/numberOflines<p>
	<p>           PMI = Log10(probpair / (probEsq*probDir)) </p>
	<p>           Emit(pair p, doub PMI)</p></li>

</ul>


