 $ hadoop jar target/seminariofinal-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.andersoncruzz.Training -input baseTraining -output modeloutput -reducers 5

$ hadoop jar target/seminariofinal-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.andersoncruzz.Classification -index modeloutput -file baseTest > saida.txt 

