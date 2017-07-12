import os

def create_negatives():
    print "negativos"
    negative = open("database/negativos.txt", "w")
    for docs in os.listdir("database/neg/"):
        print docs
        with open("database/neg/" + docs) as doc:
            doc_final = ""
            for line in doc:
                line = line.replace("\n", "")
                doc_final = doc_final + " " + line
            negative.write("neg#@!=$"+doc_final + "\n")
    negative.close()
        

def create_positives():
    print "positivos"
    positive = open("database/positivos.txt", "w")
    for docs in os.listdir("database/pos/"):
        print docs
        with open("database/pos/" + docs) as doc:
            doc_final = ""
            for line in doc:
                line = line.replace("\n", "")
                doc_final = doc_final + " " + line
            positive.write("pos#@!=$"+doc_final + "\n")
    positive.close()

def generateBase(split):
    with open ("database/merge.txt") as fp:
        bag = list()
        for line in fp:
            bag.append(line)
            #print line
        print("len")
        print(len(bag))

        percentage = (split/float(100))*float(len(bag))
        print percentage
        baseTrain = open("database/databaseHoldout/"+ str(split) + "-baseTrain.txt", "w")
        for instance in range(0,int(percentage)):
            print instance
            baseTrain.write(bag[instance])
            #print bag[instance]
        baseTrain.close()

        baseTest = open("database/databaseHoldout/"+ str(split) + "-baseTest.txt", "w")
        for instance in range(int(percentage), len(bag)):
            print instance
            baseTest.write(bag[instance])
            #print bag[instance]
        baseTest.close()

        
def merge():
    print "mergge"
    with open("database/positivos.txt") as positivos:
        with open("database/negativos.txt") as negativos:
            contPositivos = 0
            contNegativos = 0
            listPositivo = list()
            listNegativo = list()
            for line in positivos:
                contPositivos = contPositivos + 1
                listPositivo.append(line)
            for lineN in negativos:
                contNegativos = contNegativos + 1
                listNegativo.append(lineN)
            print "positivos " + str(contPositivos)
            print "negativos " + str(contNegativos)
            print "LIST positivos " + str(len(listPositivo))
            print "LIST negativos " + str(len(listNegativo))

            if len(listPositivo) == len(listNegativo):
                #print True
                merge_doc = open("database/merge.txt", "w")
                for i in range(0,len(listNegativo)):
                    merge_doc.write(listNegativo[i])
                    merge_doc.write(listPositivo[i])
                merge_doc.close()
                    

        #for line in doc:
