Não consegui fazer o Pagerank personalizado 100%.

Somente tem entrada para um "source".

Primeiro faça o BuildPageRank:

$ hadoop jar target/projeto4-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.andersoncruzz.BuildPageRankPersonalizedRecords -input sample-small.txt -output ppgrecordsfinal -numNodes 93 -source 11044077

Este programa irá atribuir massa 1 ao source e 0 aos demais como indicado por Lin.

$ hadoop fs -mkdir smallppg

$ hadoop jar target/projeto4-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.andersoncruzz.PartitionGraph -input ppgrecordsfinal -output smallppg/iter0000 -numPartitions 5 -numNodes 93

$ hadoop jar target/projeto4-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.andersoncruzz.RunPageRankPersonalizedBasic -base smallppg -numNodes 93 -start 0 -end 20 -source 11044077


Este programa RunPageRankPersonalizedBasic não está funcionando como deveria.
Tentei adicionar a fórmula do teletransporte 1/|S| como diz nos artigos indicados, para o jump ir para o "source",
e não para qualquer caminho aleatório.

Acredito que não está funcionando adequadamente por não conseguir calcular corretamente a massa perdida e distribuir essa
massa perdida somente para o nó "source", como indicado por Lin.

$ hadoop jar target/projeto4-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.andersoncruzz.ExtractTopPersonalizedPageRankNodes -input smallppg/iter0020 -output smallppgrank_extracted

Comparando a saída deste programa com o pagerank personalizado sequencial do Lin, vemos que o meu trabalho estão com valores de probabilidade elevados, isto é, os valores não batem.
Mas até que a maioria da saída dos 10 maiores valores de pagerank está batendo com a saída do meu programa.

Acredito que o meu erro está no RunPageRankPersonalizedBasic mencionado acima.

