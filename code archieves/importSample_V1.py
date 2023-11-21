from fileinput import filename
from neo4j import GraphDatabase
from posixpath import basename
import json
import os
from typing import Set

data_base_connection = GraphDatabase.driver(uri = "bolt://localhost:7687", auth=("neo4j", "123bendageekW@"))
session = data_base_connection.session()   
pathOrgane = "./data/organe" #Chemin d'acces vers les fichiers json des organes
pathActeur = "./data/acteur" #Chemin d'acces vers les fichiers json des deputés
selectedOrganeType = ["API","ASSEMBLEE","CMP","CNPE","CNPS","COMNL","COMPER","COMSENAT","COMSPSENAT","CONFPT","CONSTITU","GA","GOUVERNEMENT","GP","GROUPESENAT","MINISTERE","ORGAINT","ORGEXTPARL","PARPOL","PRESREP","SENAT"]
organeList = []
selectedOrganIdList = []  #variable pour contenir les id des  organes issues de la limitation (#deputées et numbre de types voulus)
#sample limitation variables 
deputeeNumberLimit = 10
organTypeNumberLimite = 3
totalOrganescreated = 0


def sanitizeQuery(query):    
    return '"'+query+'"'


def storeOrganesInNeo():     
    #add primary
    try:
        query_unique_constraint = "CREATE CONSTRAINT ON (organe:Organe) ASSERT organe.uid IS UNIQUE"
        query_exists_constraint = "CREATE CONSTRAINT ON (organe:Organe) ASSERT exists(organe.uid)"
        session.run(query=query_unique_constraint)
        session.run(query=query_exists_constraint)
    except :
        print('Already created constraint') 

    count = 0
    for organe in organeList:        
        #create organes
        query = "Create (o:Organe{uid:'"+ str(organe[0]) +"' , codeType:'"+ str(organe[1]) +"' , libelle:\""+ str(organe[2]) +"\", dateDebut : '"+ str(organe[3]) + "', dateFin : '"+ organe[4]+ "'})"
        session.run(query=query)
        #count+=1
    #print(count)  
    print("done adding to database")
    
              

def getOrganeProperties(fileName): #methode pour lire le fichier json
    with open(fileName) as file:
        data = json.load(file)
        if data['organe'].get("uid") in selectedOrganIdList:
            organeTampon = []
            organeTampon.append(data['organe'].get("uid"))
            organeTampon.append(data['organe'].get("codeType"))
            organeTampon.append((data['organe'].get("libelle")))
            organeTampon.append(data['organe'].get("viMoDe").get("dateDebut",""))
            if(data['organe'].get("viMoDe").get("dateFin")): #Gestion des dates de fin null
               dateFin = data['organe'].get("viMoDe").get("dateFin")  
            else:
                dateFin ="" 
            organeTampon.append(dateFin)
            organeList.append(organeTampon)
                    

def getAllOrgansJsonFiles(basepath) : #methode pour detecter automatiquement tous les fichiers csv dans le reportoire passé en parametre
    i = 0  
    for entry in os.listdir(basepath):
        if os.path.isfile(os.path.join(basepath, entry)):
            extension = os.path.splitext(entry)[1]
            
            if extension == ".json":                
                getOrganeProperties(os.path.join(basepath, entry))                
                #print("  SUCCESSFULLY EXTRACTED ORGANES FROM "+ entry)

def getAndStoreOrganes(basepath):
    getAllOrgansJsonFiles(basepath)  
    storeOrganesInNeo()            
        

def IdentifyRequiredOrganes(basepath):
    counter = 1
    for entry in os.listdir(basepath):
        if os.path.isfile(os.path.join(basepath, entry)):
            extension = os.path.splitext(entry)[1]            
            if extension == ".json":
                if counter <=deputeeNumberLimit: 

                    filename = os.path.join(basepath, entry)
                    with open(filename) as file:
                        data = json.load(file)
                        allMandats =  data['acteur'].get("mandats").get("mandat")
                        for mandat in allMandats:
                            if mandat.get("typeOrgane") in selectedOrganeType[0:organTypeNumberLimite]: #on prends les organes qui appartiennent à la liste reduite de type d'organes
                                selectedOrganIdList.append(mandat.get("organes").get("organeRef"))
                    counter +=1
    

if __name__ == "__main__":    
    print("\n=== PROJET NEO4J - IMPORT SAMPLE ORGANES, ACTORS AND RELATIONSHIPS IN NEO4J===\n")  

             #1- IDENTIFY THE ORGANS CORRESPONDING TO THE n FIRST DEPUTEES AND m FIRST ORGANE TYPES
    IdentifyRequiredOrganes(pathActeur)
    #print(len(selectedOrganIdList))

            #2- GET AND STORE THE IDENTIFIED ORGANS
    getAndStoreOrganes(pathOrgane)
    #print(len(organeList))


            #3-CREATE FIRST n DEPUTEES NODES AND CREATE CORRESPONDING RELATIONSHIPS

    
    session.close()   
    print("\n=== PROJET NEO4J - IMPORT SAMPLE ORGANES, ACTORS AND RELATIONSHIPS IN NEO4J ===\n")
    





















