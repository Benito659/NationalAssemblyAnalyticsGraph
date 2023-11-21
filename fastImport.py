from fileinput import filename
from neo4j import GraphDatabase
from posixpath import basename
import json
import os
from typing import Set


# data_base_connection = GraphDatabase.driver(uri = "bolt://neo4j:7687", auth=("neo4j", "123narutoC."))

import os
db_host = os.environ.get('NEO4J_HOST', 'localhost')
db_port = os.environ.get('NEO4J_PORT', '7687')
print("db_host == ",db_host,"db_port == ",db_port)
data_base_connection = GraphDatabase.driver(uri='bolt://project-neo4jplus-1:7687',auth=("neo4j", "123narutoC."))



session = data_base_connection.session()

pathOrgane = "./data/organe" #Chemin d'acces vers les fichiers json des organes
pathActeur = "./data/acteur" #Chemin d'acces vers les fichiers json des deputés
pathAmendement ="./data/amendement/amendementList.json"
pathDossier ="./data/dossierLegislatif/dossierLegislatifList.json"


actorsList = []
organeList = []
actorOrganLinkList = []
selectedOrganIdList = []  #variable pour contenir les id des  organes issues de la limitation (#deputées et numbre de types voulus)
#sample limitation variables 
deputeeNumberLimit = 10000
organTypeNumberLimit = 10
totalOrganescreated = 0
amendementList=[]
#relation: soumet
#relation : vote & amendement
actorAndAmendementLinkedList=[]
#lien amendement/list
dossierAndAmendementLinkedList=[]
dossierList=[]
texteDeLoiLinkAmendement=[]


def getAmendementProperties(fileName): #methode pour lire le fichier json
    with open(fileName) as file:
        data = json.load(file)
        # if data['organe'].get("uid") in selectedOrganIdList:
        for amendement in data : 
            AmendementTampon = {
                "uid": amendement.get("uid"),
                "dateDepot": amendement.get("dateDepot"),
                "datePublication": amendement.get("datePublication") if (type(amendement.get("datePublication"))!=type({})) else None,
                "dateVoteFinal": amendement.get("dateVoteFinal") if (type(amendement.get("dateVoteFinal"))!=type({})) else None,
                "resultatVoteFinal": amendement.get("resultatVoteFinal") if (type(amendement.get("resultatVoteFinal"))!=type({})) else None,
                "texteLegislatifRef": amendement.get("texteLegislatifRef"),
                "division": amendement.get("division"),
            }
            DossierTampon={
                "AmandementId": amendement.get("uid"),
                "DossierId": amendement.get("texteLegislatifRef"),
            }
            listCoActorInAmendement=[]
            for j in amendement.get("cosignataires"):
                listCoActorInAmendement.append(j)
            ActorTampon={
                "to": amendement.get("uid"),
                "from": amendement.get("signataires"),
                "properties":{"cosignataires":listCoActorInAmendement}
            }

            TexteDeLoiTampon={
                "AmandementId": amendement.get("uid"),
                "texteDeLoi": amendement.get("texteDeLoi"),
            }
            amendementList.append(AmendementTampon)
            dossierAndAmendementLinkedList.append(DossierTampon)
            if(type(amendement.get("signataires"))==type("")) and (type(amendement.get("uid"))==type("")) :
                actorAndAmendementLinkedList.append(ActorTampon)
            texteDeLoiLinkAmendement.append(TexteDeLoiTampon)

def storeAmendementActorLinkInNeoSample():   
    printDev = True
    a,b=0,8000
    for i in range (b):
        try:
            queryGlobal=("UNWIND "+str(actorAndAmendementLinkedList[a:a+len(actorAndAmendementLinkedList)//(b)])+" as batch ").replace("'from'","from").replace("'to'","to").replace("'properties'","properties").replace("'cosignataires'","cosignataires")
            querySecond="match(from:Acteur{uid:batch.from}) match (to:Amendement{uid:batch.to}) merge (from)-[l:Soumet] -> (to)  ON CREATE SET l+= batch.properties"
            a=a+len(actorAndAmendementLinkedList)//(b)
            #for link in actorAndAmendementLinkedList:
            #    query = "match(a:Acteur{uid:'"+str(link['ActorId'])+"'}) ,(o:Amendement{uid:'"+(str(link["AmandementId"]) )+"'}) merge (a)-[l:Soumet{cosignataires:"+str(link['CosignataireId'])+"}] -> (o) ;"
            #    session.run(query=query)
            #    if printDev: print("\t -> Création de la relation  ", link['ActorId'], "-> ", link["AmandementId"]," OK")
            session.run(query=(queryGlobal+querySecond))
            print(((a)/len(actorAndAmendementLinkedList))*100)
        except ValueError :
            print(ValueError)
    if printDev: print("Fin des rélations acteurs --> amendements")


def lancheringestion():   
    print("\n=== PROJET NEO4J - IMPORT SAMPLE ORGANES, ACTORS AND RELATIONSHIPS IN NEO4J===\n")  

    
    getAmendementProperties(pathAmendement)
    #storeAmendementInNeo()
    storeAmendementActorLinkInNeoSample()
    #getDossierProperties(pathDossier)
    #storeDossierLegislatifInNeo()
    #storeAmendementDossierLegislatifLinkInNeo()



    
    session.close()   
    print("\n=== PROJET NEO4J - IMPORT SAMPLE ORGANES, ACTORS AND RELATIONSHIPS IN NEO4J ===\n")
