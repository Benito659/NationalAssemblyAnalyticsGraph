
from neo4j import GraphDatabase

# data_base_connection = GraphDatabase.driver(uri = "bolt://neo4j:7687", auth=("neo4j", "123narutoC."))

import os
db_host = os.environ.get('NEO4J_HOST', 'localhost')
db_port = os.environ.get('NEO4J_PORT', '7687')
print("db_host == ",db_host,"db_port == ",db_port)
data_base_connection = GraphDatabase.driver(uri='bolt://project-neo4jplus-1:7687',auth=("neo4j", "123narutoC."))



session = data_base_connection.session()

defaultPic = "assets/images/avatars/avatar.png"


def testDataSet():#test if dataset has dateset has been imported
    isPresent = False
    try:
        query = "MATCH (n:Organe) return n"
        if(len(session.run(query).data())) >  0:
            isPresent = True
    except:
        pass
    return isPresent

def getTexteWithMostVotesContres(limit):
    query= "MATCH (a:Acteur)-[l:VOTEDFOR{positionVote:'contre'}]-> (t:Texte_de_loi) return t as texte, count(*) as votes order by votes desc limit  $max"
    resultats = session.run(query=query,max=(limit+limit))
    resultList = []
    counter =  1 
    
    for result in resultats: 
        if counter%2!=0: #turn around temporaire pour les doublons
            row = {
                "titre" : result["texte"]._properties.get("titrePrincipal"),
                "number"  : result["votes"],                
            }    
            resultList.append(row)           
        counter+=1
        
    return resultList

def getTexteWithMostAbstentions(limit):
    query= "MATCH (a:Acteur)-[l:VOTEDFOR{positionVote:'abstention'}]-> (t:Texte_de_loi) return t as texte, count(*) as votes order by votes desc limit  $max"
    resultats = session.run(query=query,max=(limit+limit))
    resultList = []
    counter =  1 
    
    for result in resultats: 
        if counter%2!=0: #turn around temporaire pour les doublons
            row = {
                "titre" : result["texte"]._properties.get("titrePrincipal"),
                "number"  : result["votes"],               
            }    
            resultList.append(row)
        counter+=1
    return resultList

def getMostVoteDelegator(limit):
    query= "MATCH (a:Acteur)-[l:VOTEDFOR{parDelegation:'true'}]-> (t:Texte_de_loi) return a as acteur, count(*) as votesParDelegation order by votesParDelegation desc limit $max"
    resultats = session.run(query=query,max=limit)
    resultList = []
    for result in resultats:
        acteur = result["acteur"]        
        row = {
            "picture" : acteur._properties.get("picture",defaultPic),
            "nom" : acteur._properties.get("nom"),
            "prenom" : acteur._properties.get("prenom"),
            "profession" : acteur._properties.get("profession") if "@" not in acteur._properties.get("profession") else "Sans profession déclarée",
            "nombre" : result["votesParDelegation"]            
        }    
        resultList.append(row)
    return resultList 


def getPartiesWithMostOppositionToTexte(limit):
    query= "MATCH (o:Organe)-[v:VOTEDFOR]->(dl:DossierLegislatif) WHERE v.positionMajoritaire='contre' AND dl.procedureParlementaire='Projet de loi ordinaire' WITH COUNT(o) as nb, o ORDER BY nb DESC RETURN nb as ctn, o as organe LIMIT $max"
    resultats = session.run(query=query,max=limit)
    resultList = []
    counter =  1 
    for result in resultats: 
        row = {
            "libelle" : result["organe"]._properties.get("libelle"),
            "number"  : result["ctn"],
            "index": counter
        }    
        resultList.append(row)
        counter+=1
    return resultList

def gettextWithMostExternalSupport(limit):
    query= "match (o:Organe{codeType:'PARPOL'})<-[:ISPARTIPOLITIQUE]-(Signataire:Acteur)- [:SIGNS]->(am:Amendement)<-[:COSIGNS] - (Cosignataire:Acteur) -[:ISPARTIPOLITIQUE]->(os:Organe{codeType:'PARPOL'}) where o.libelle<>os.libelle return o.libelle as PartiPolitique_signataire, os.libelle as PartiPolitique_cosignataire, COUNT(am) as nb_amendements ORDER BY nb_amendements DESC LIMIT $max"
    resultats = session.run(query=query,max=limit)
    resultList = []    
    for result in resultats: 
        row = {
            "partieSigner" : result["PartiPolitique_signataire"],
            "partieCosigner"  : result["PartiPolitique_cosignataire"],
            "number": result["nb_amendements"]
        }    
        resultList.append(row)        
    return resultList


#def getArticleWithMosttexts(limit):
    #query= "match (o:Organe{codeType:'PARPOL'})<-[:ISPARTIPOLITIQUE]-(Signataire:Acteur)- [:SIGNS]->(am:Amendement)<-[:COSIGNS] - (Cosignataire:Acteur) -[:ISPARTIPOLITIQUE]->(os:Organe{codeType:'PARPOL'}) where o.libelle<>os.libelle return o.libelle as PartiPolitique_signataire, os.libelle as PartiPolitique_cosignataire, COUNT(am) as nb_amendements ORDER BY nb_amendements DESC LIMIT $max"

    
def getPartiesWithMostMinister(limit):
    query= "MATCH p=(n:Acteur)-[:Ministre]->(o:Organe) with n match (:Acteur{uid:n.uid})-[:Membre]->(o:Organe{codeType:'PARPOL'}) return o as organe, Count(*) as ctn ORDER BY ctn desc LIMIT $max"
    resultats = session.run(query=query,max=limit)
    resultList = []
    counter =  1 
    for result in resultats: 
        row = {
            "libelle" : result["organe"]._properties.get("libelle"),
            "number"  : result["ctn"],
            "index": counter
        }    
        resultList.append(row)
        counter+=1
    return resultList

def getMostChangePartyPol(limit):
    query= "MATCH p=(a:Acteur)-[:ISPARTIPOLITIQUE]->(:Organe{codeType:'GP'}) WITH a as acteur, Count(*) as ctn ORDER BY ctn desc RETURN acteur, ctn LIMIT $max"
    resultats = session.run(query=query,max=limit)
    resultList = []
    for result in resultats:
        acteur = result["acteur"]        
        row = {
            "picture" : acteur._properties.get("picture",defaultPic),
            "nom" : acteur._properties.get("nom"),
            "prenom" : acteur._properties.get("prenom"),
            "profession" : acteur._properties.get("profession") if "@" not in acteur._properties.get("profession") else "Sans profession déclarée",
            "nombre" : result["ctn"]            
        }    
        resultList.append(row)
    return resultList 

def getEarliestDead(limit):
    query="MATCH (n:Acteur)-[m:Membre]->(o:Organe) WHERE n.dateDeces IS NOT NULL AND n.dateDeces <>'None' AND  m.legislature<>'None' AND o.codeType='COMPER' WITH n, m, o, date(n.dateDeces) AS dateMort,date(CASE WHEN m.legislature = '15' THEN '2017-06-21' WHEN m.legislature = '14' THEN '2012-06-21' WHEN m.legislature = '13' THEN '2007-06-21' WHEN m.legislature = '12' THEN '2002-06-21' WHEN m.legislature = '11' THEN '1997-06-21' ELSE '1000-01-01' END) as dateDebutMandat RETURN DISTINCT n as acteur, (dateMort.year - dateDebutMandat.year)*360 + toFloat(dateMort.month-dateDebutMandat.month)*30 + toFloat(dateMort.day - dateDebutMandat.day) as dureeMandat, dateDebutMandat as startDate, dateMort ORDER BY dureeMandat LIMIT $max"
    resultats = session.run(query=query,max=limit)
    resultList = []
    for result in resultats:
        acteur = result["acteur"]        
        row = {
            "picture" : acteur._properties.get("picture",defaultPic),
            "nom" : acteur._properties.get("nom"),
            "prenom" : acteur._properties.get("prenom"),
            "profession" : acteur._properties.get("profession") if "@" not in acteur._properties.get("profession") else "Sans profession déclarée",
            "startDate": result["startDate"],
            "deathDate": result["dateMort"],
            "duree": str(result).split("=")[5].split(".")[0]
        }    
        resultList.append(row)
    return resultList

def getMostVoteDifferentFromParpol(limit):
    query= "MATCH (a:Acteur)-[l1:VOTEDFOR]-> (t:Texte_de_loi) <-[l2:VOTEDFOR]-(o:Organe)<-[:ISPARTIPOLITIQUE]-(a:Acteur) WITH  a ,l1, l2, o, t WHERE l1.voteId =l2.voteId AND l1.positionVote <>l2.positionMajoritaire AND l1.positionVote<>'nonVotant' AND o.libelle<>'Non inscrit' RETURN a as acteur, count(*) as nbVotesDif ORDER BY nbVotesDif desc limit $max"
    resultats = session.run(query=query,max=limit)
    resultList = []
    for result in resultats:
        acteur = result["acteur"]        
        row = {
            "picture" : acteur._properties.get("picture",defaultPic),
            "nom" : acteur._properties.get("nom"),
            "prenom" : acteur._properties.get("prenom"),
            "profession" : acteur._properties.get("profession") if "@" not in acteur._properties.get("profession") else "Sans profession déclarée",
            "nombre": result["nbVotesDif"]
        }    
        resultList.append(row)
    return resultList

def getMostVoteUniqueDifferentFromParpol(limit):
    query= "MATCH (a:Acteur)-[l1:VOTEDFOR]-> (t:Texte_de_loi) <-[l2:VOTEDFOR]-(o:Organe)<-[:ISPARTIPOLITIQUE]-(a:Acteur) WITH  a ,l1, l2, o, t WHERE l1.voteId =l2.voteId AND l1.positionVote <>l2.positionMajoritaire AND l1.positionVote<>'nonVotant' AND o.libelle<>'Non inscrit' AND ((l2.decompteVoix_contre='1' AND l1.positionVote='contre') OR (l2.decompteVoix_pour='1' AND l1.positionVote='pour')) RETURN a as acteur, count(*) as nbVotesDif ORDER BY nbVotesDif desc limit $max"
    resultats = session.run(query=query,max=limit)
    resultList = []
    for result in resultats:
        acteur = result["acteur"]        
        row = {
            "picture" : acteur._properties.get("picture",defaultPic),
            "nom" : acteur._properties.get("nom"),
            "prenom" : acteur._properties.get("prenom"),
            "profession" : acteur._properties.get("profession") if "@" not in acteur._properties.get("profession") else "Sans profession déclarée",
            "nombre": result["nbVotesDif"]
        }    
        resultList.append(row)
    return resultList

def getYoungestActors(limit):
    query = "MATCH (n:Acteur)-[m:Membre]->(o:Organe) WHERE m.legislature<>'None' AND o.codeType='COMPER' WITH n, m, o, date(n.dateNais) AS dateNaissance,date(CASE WHEN m.legislature = '15' THEN '2017-06-21' WHEN m.legislature = '14' THEN '2012-06-21' WHEN m.legislature = '13' THEN '2007-06-21' WHEN m.legislature = '12' THEN '2002-06-21' WHEN m.legislature = '11' THEN '1997-06-21' ELSE '1000-01-01' END) as dateDebutMandat RETURN DISTINCT n as acteur, ( (dateDebutMandat.year-dateNaissance.year) + toFloat(dateDebutMandat.month-dateNaissance.month)/12 + toFloat(dateDebutMandat.day-dateNaissance.day)/365 )as ageDeputeDebutMandat ORDER BY ageDeputeDebutMandat LIMIT $max"
    resultats = session.run(query=query,max=limit)
    resultList = []
    for result in resultats:
        acteur = result["acteur"]        
        row = {
            "picture" : acteur._properties.get("picture",defaultPic),
            "nom" : acteur._properties.get("nom"),
            "prenom" : acteur._properties.get("prenom"),
            "profession" : acteur._properties.get("profession") if "@" not in acteur._properties.get("profession") else "Sans profession déclarée",
            "age": str(result).split("=")[5].split(".")[0]
        }    
        resultList.append(row)
    return resultList
    

def testQuery():
    query = "MATCH (a:Organe) RETURN a.uid, a.codeType limit 3"
    resultats = session.run(query=query)
    resultList = []

    for result in resultats:
        row = {
            "id" : result["a.uid"],
            "type": result["a.codeType"] 
        }    
        resultList.append(row)
    return resultList


'''
for item in resultList:
    pass
    print(item.get('id'))
#len(resultList)
print("Number of results gotten = "+str(len(resultList)))
'''


