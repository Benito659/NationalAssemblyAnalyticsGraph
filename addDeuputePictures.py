import csv
from neo4j import GraphDatabase

# data_base_connection = GraphDatabase.driver(uri = "bolt://neo4j:7687", auth=("neo4j", "123narutoC."))
import os
# project-neo4jplus-1
db_host = os.environ.get('NEO4J_HOST', 'localhost')
db_port = os.environ.get('NEO4J_PORT', '7687')
print("db_host == ",db_host,"db_port == ",db_port)
data_base_connection = GraphDatabase.driver(uri='bolt://project-neo4jplus-1:7687',auth=("neo4j", "123narutoC."))


session = data_base_connection.session()


#variables générales: 
sourceFile = "data/deputesList_photos.csv" #fichier d'entrée pour notre table de conversion
conversionTable = []   #liste qui contiendra la table de conversion


def addPictures():
    print("\nDébut d'ajout des photos pour les noeuds acteurs")
    counter = 0
    for data in conversionTable:        
        query = "MATCH (a:Acteur {uid: '"+str(data.get("uid"))+ "'}) SET a.picture = '"+str(data.get("picture"))+"'"
        session.run(query=query)
        counter+=1
        if  counter%100==0 : print("Nombres de photos stocquées dans les noeuds acteur : ",counter ,"/ 1415")
    print("Fin d'ajout des photos pour les noeuds acteurs")

def buildConversionTable(sourceFileName):
    with open(sourceFileName) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',') 
        next(csv_reader) #permet de sauter la première colonne qui contient l'entête 
        i = 1 #counter    
        for row in csv_reader:
            entry = {
                "uid": row[3],
                "picture" : row[4]
            }
            conversionTable.append(entry)
            i+=1
            if  i%100==0 : print("Nombres de photos stocquées : ",i ,"/ 1415")

def lancheringestion(): 
    print("\n=== PROJET NEO4J - ADD PICTURE TO DEPUTE NODES ===\n")   
    buildConversionTable(sourceFile)
    addPictures()
    print("\n\n")
    
    #print(conversionTable[:2])
    #print(len(conversionTable) )

    print("\nALL PICTURES PATHS ADDED SUCCESFULLY")   
    print("\n=== PROJET NEO4J - ADD PICTURE TO DEPUTE NODES ===\n")