
from neo4j import GraphDatabase
import json 

# data_base_connection = GraphDatabase.driver(uri = "bolt://neo4j:7687", auth=("neo4j", "123narutoC."))

import os
db_host = os.environ.get('NEO4J_HOST', 'localhost')
db_port = os.environ.get('NEO4J_PORT', '7687')
data_base_connection = GraphDatabase.driver(uri='bolt://project-neo4jplus-1:7687',auth=("neo4j", "123narutoC."))
print("db_host == ",db_host,"db_port == ",db_port)

session = data_base_connection.session()

queryFolder = "./data/requetes"
queryYoungest = queryFolder+"/queryYoungest.json"
queryChangers = queryFolder+"/queryChangers.json"
queryMinistersParties = queryFolder+"/queryMinistersParties.json"
queryDelegators = queryFolder+"/queryDelegators.json"
queryAbstentions = queryFolder+"/queryAbstentions.json"
queryContestations= queryFolder+"/queryContestations.json"
queryEarlyDeaths = queryFolder+"/queryEarlyDeaths.json"
queryDiffVoters = queryFolder+"/queryDiffVoters.json"
queryDiffUniqueVoters = queryFolder+"/queryDiffUniqueVoters.json"
queryOpposants = queryFolder+"/queryOpposants.json"
queryExternalSupport= queryFolder+"/queryExternalSupport.json"
queryMostAmandement = queryFolder+"/queryMostAmandement.json"
defaultPic = "assets/images/avatars/avatar.png"




def getYoungestActors(limit):
    with open(queryYoungest) as file:
        data = json.load(file)    
        resultList = []
        counter = 0
        for result in data:            
            acteur = result["acteur"]        
            row = {
                "picture" : acteur.get("properties").get("picture",defaultPic),
                "nom" : acteur.get("properties").get("nom"),
                "prenom" : acteur.get("properties").get("prenom"),
                "profession" : acteur.get("properties").get("profession") if "@" not in acteur.get("properties").get("profession") else "Sans profession déclarée",
                "age": str(result["ageDeputeDebutMandat"])[:-13]
            }    
            resultList.append(row)
            counter+=1
            if counter >= limit:
                break
        return resultList

def getMostChangePartyPol(limit):
    with open(queryChangers) as file:
        data = json.load(file)
        counter = 0
        resultList = []
        for result in data:
            acteur = result["acteur"]        
            row = {
                "picture" : acteur.get("properties").get("picture",defaultPic),
                "nom" : acteur.get("properties").get("nom"),
                "prenom" : acteur.get("properties").get("prenom"),
                "profession" : acteur.get("properties").get("profession") if "@" not in acteur.get("properties").get("profession") else "Sans profession déclarée",
                "nombre" : result["ctn"]            
            }    
            resultList.append(row)
            counter+=1
            if counter >= limit:
                break
        return resultList 

def getPartiesWithMostMinister(limit):
    with open(queryMinistersParties) as file:
        data = json.load(file)
        index = 0        
        resultList = []
        counter =  0 
        for result in data: 
            row = {
                "libelle" : result["organe"].get("properties").get("libelle"),
                "number"  : result["ctn"],
                "index": counter
            }    
            resultList.append(row)
            index+=1
            counter+=1
            if counter >= limit:
                break
        return resultList

def getMostVoteDelegator(limit):
    with open(queryDelegators) as file:
        data = json.load(file)
        counter = 0
        resultList = []    
        for result in data:
            acteur = result["acteur"]        
            row = {
                "picture" : acteur.get("properties").get("picture",defaultPic),
                "nom" : acteur.get("properties").get("nom"),
                "prenom" : acteur.get("properties").get("prenom"),
                "profession" : acteur.get("properties").get("profession") if "@" not in acteur.get("properties").get("profession") else "Sans profession déclarée",
                "nombre" : result["votesParDelegation"]            
            }    
            resultList.append(row)
            counter+=1
            if counter >= limit:
                break
        return resultList 

def getTexteWithMostAbstentions(limit):
    with open(queryAbstentions) as file:
        data = json.load(file)             
        resultList = []
        max = 0
        counter =  0 
        for result in data: 
            if counter%2!=0: #turn around temporaire pour les doublons
                row = {
                    "titre" : result["texte"].get("properties").get("titrePrincipal"),
                    "number"  : result["votes"],               
                }    
                resultList.append(row)
                max+=1            
            counter+=1
            if max >= limit:
                break
        return resultList


def getTexteWithMostVotesContres(limit):
    with open(queryContestations) as file:
        data = json.load(file)             
        resultList = []
        max = 0
        counter =  0 
        for result in data: 
            if counter%2!=0: #turn around temporaire pour les doublons
                row = {
                    "titre" : result["texte"].get("properties").get("titrePrincipal"),
                    "number"  : result["votes"],               
                }    
                resultList.append(row)
                max+=1            
            counter+=1
            if max >= limit:
                break
        return resultList


def getEarliestDead(limit):
    with open(queryEarlyDeaths) as file:
        data = json.load(file)
        counter = 0
        resultList = []
        for result in data:
            acteur = result["acteur"]        
            row = {                
                "nom" : acteur.get("properties").get("nom"),
                "picture" : acteur.get("properties").get("picture",defaultPic),
                "prenom" : acteur.get("properties").get("prenom"),
                "profession" : acteur.get("properties").get("profession") if "@" not in acteur.get("properties").get("profession") else "Sans profession déclarée",
                "duree": result["dureeMandat"]
            }    
            resultList.append(row)
            counter+=1
            if counter >= limit:
                break
        return resultList


def getMostVoteDifferentFromParpol(limit):
    with open(queryDiffVoters) as file:
        data = json.load(file)
        counter = 0
        resultList = []
        for result in data:
            acteur = result["acteur"]        
            row = {
                "picture" : acteur.get("properties").get("picture",defaultPic),
                "nom" : acteur.get("properties").get("nom"),
                "prenom" : acteur.get("properties").get("prenom"),
                "profession" : acteur.get("properties").get("profession") if "@" not in acteur.get("properties").get("profession") else "Sans profession déclarée",
                "nombre": result["nbVotesDif"]
            }    
            resultList.append(row)
            counter+=1
            if counter >= limit:
                break
        return resultList


def getMostVoteUniqueDifferentFromParpol(limit):
    with open(queryDiffUniqueVoters) as file:
        data = json.load(file)
        counter = 0
        resultList = []
        for result in data:
            acteur = result["acteur"]        
            row = {
                "picture" : acteur.get("properties").get("picture",defaultPic),
                "nom" : acteur.get("properties").get("nom"),
                "prenom" : acteur.get("properties").get("prenom"),
                "profession" : acteur.get("properties").get("profession") if "@" not in acteur.get("properties").get("profession") else "Sans profession déclarée",
                "nombre": result["nbVotesDif"]
            }    
            resultList.append(row)
            counter+=1
            if counter >= limit:
                break
        return resultList



def getPartiesWithMostOppositionToTexte(limit):
    with open(queryOpposants) as file:
        data = json.load(file)   
        resultList = []
        counter =  1 
        for result in data: 
            row = {
                "libelle" : result["organe"].get("properties").get("libelle"),
                "number"  : result["nb"],
                "index": counter
            }    
            resultList.append(row)
            counter+=1
            if counter >= limit:
                break            
        return resultList

def getAmandementWithMostExternalSupport(limit):
    with open(queryExternalSupport) as file:
        data = json.load(file)
        resultList = []    
        counter = 0
        for result in data: 
            row = {
                "partieSigner" : result["PartiPolitique_signataire"],
                "partieCosigner"  : result["PartiPolitique_cosignataire"],
                "number": result["nb_amendements"]
            }    
            resultList.append(row) 
            counter+=1
            if counter >= limit:
                break        
        return resultList


def getArticleWithMostAmandements(limit):
    with open(queryMostAmandement) as file:
        data = json.load(file)
        resultList = []    
        counter = 0
        for result in data: 
            row = {
                "titre" : result["titre"],
                "article"  : result["article"],
                "number": result["nombreArticle"]
            }    
            resultList.append(row) 
            counter+=1
            if counter >= limit:
                break        
        return resultList










