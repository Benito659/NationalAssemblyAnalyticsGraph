# -*- coding: utf-8 -*-
"""
Created on Fri Mar  4 21:26:48 2022

@author: debat
"""

from fileinput import filename
from neo4j import GraphDatabase
from posixpath import basename
import json
import os
from typing import Set
import time

# data_base_connection = GraphDatabase.driver(uri = "bolt://neo4j:7687", auth=("neo4j", "123narutoC."))

import os
db_host = os.environ.get('NEO4J_HOST', 'localhost')
db_port = os.environ.get('NEO4J_PORT', '7687')
print("db_host == ",db_host,"db_port == ",db_port)
data_base_connection = GraphDatabase.driver(uri='bolt://project-neo4jplus-1:7687',auth=("neo4j", "123narutoC."))



session = data_base_connection.session()

pathdata ="data/"
path =pathdata+"dossierLegislatif/"


def linkTexteDeLoiToScrutin():
    with open('texteDeLoiList.json') as json_data:
        texteDeLoiList = json.load(json_data)
    
    with open(pathdata+'scrutinList.json') as json_data:
        scrutinList = json.load(json_data)
    
    print("\ntexteDeLoiList len : ", len(texteDeLoiList))
    print("\nscrutinList len : ", len(scrutinList))
    
    if False:
        print("\n", texteDeLoiList[0].get("titrePrincipal"))
        print("\n", texteDeLoiList[0].get("titrePrincipalCourt"))        
        print("\n", scrutinList[0].get("titre"))
    
    if False:
        ind1=0
        for eachVote in scrutinList:
            ind1+=1
            if ind1 < 2:
                # print("\n -> titre : ", eachVote.get("titre") )
                
                ind2=0
                for eachTexteDeLoi in texteDeLoiList:
                    ind2+=1
                    if ind2<4:
                        print("\ntitrePrincipal : ", eachTexteDeLoi.get("titrePrincipal"))
                    if eachVote.get("titre") in eachTexteDeLoi.get("titrePrincipal"):
                        print(eachVote.get("uid"))
    
    voteTexteDeLoiList=[]
    indTexteDeLoi=1104
    counter=0
        
    for eachVote in scrutinList:
        counter+=1
        if counter<100000:
            tempTexteDeLoi=[]
            for eachTexteDeLoi in texteDeLoiList:
                if eachTexteDeLoi.get("titrePrincipal") in eachVote.get("titre"):
                    if False:
                        print("\n titrePrincipal : ", eachTexteDeLoi.get("titrePrincipal") )
                        print("\n titre : ", eachVote.get("titre") )
                        wait = input("Press enter")
                    tempTexteDeLoi.append(eachTexteDeLoi)
                    
            voteTexteDeLoiList.append({
                'vote': {
                    'uid': eachVote.get("uid"), 
                    'organeRef': eachVote.get("organeRef"), 
                    'sessionRef': eachVote.get("sessionRef"), 
                    'seanceRef': eachVote.get("seanceRef"), 
                    'dateScrutin': eachVote.get("dateScrutin"), 
                    'typeVote': eachVote.get("libelleTypeVote"), 
                    'titre': eachVote.get("titre"), 
                    'objet': eachVote.get("objet"), 
                    },
                'texteDeLoi': tempTexteDeLoi,
                })
    
    
    if True: 
        with open(pathdata+'voteTexteDeLoiList.json', 'w') as g2:
            json.dump(voteTexteDeLoiList, g2)
    
    print("\n\nvoteTexteDeLoiList : ", voteTexteDeLoiList)
    print("\n\ncounter : ", counter)



def cleanLibelle(libelle):
    return libelle.replace('"',"'")


def storeDossiersLegislatifsInNeo():   
    t = time.time()
    with open(path+'dossierLegislatifList.json') as json_data:
        dossierLegislatifList = json.load(json_data)
    
    printDev = True
    if printDev: print("\n\n\nCREATION DES NOEUDS 'DOSSIER LEGISLATIF' DANS NEO4J :\n")
    
    #add primary
    try:
        query_unique_constraint = "CREATE CONSTRAINT ON (dossierLegislatif:DossierLegislatif) ASSERT dossierLegislatif.uid IS UNIQUE"
        query_exists_constraint = "CREATE CONSTRAINT ON (dossierLegislatif:DossierLegislatif) ASSERT exists(dossierLegislatif.uid)"
        session.run(query=query_unique_constraint)
        session.run(query=query_exists_constraint)
    except :
        print('Already created constraint') 

    ind=0
    for dossierLegislatif in dossierLegislatifList:   
        ind+=1
        #create organes
        query = "merge (dl:DossierLegislatif{uid:\""+ str(dossierLegislatif['uid']) +"\" , titre:\""+ cleanLibelle(str(dossierLegislatif['titre'])) +"\" , procedureParlementaire:\""+ cleanLibelle(str(dossierLegislatif['procedureParlementaire'])) +"\"});"
        session.run(query=query)
        if False: print("\t -> Création du noeud ", dossierLegislatif['uid'], " OK")
        
    if printDev: print("\nFin de la création des noeuds dossierLegislatif")
    elapsed = time.time() - t
    print("\nNombre de noeuds créés : ", ind, " en ", round(elapsed,2), " s")


def getDossierlegVoteLinks():
    printDev = False
    print("\n ** Récupération des liens entre dossiers législatifs et vote **\n")
    with open(path+'dossierLegislatifList.json') as json_data:
        dossierLegislatifList = json.load(json_data)
    
    dossierLegislatifVoteLinks = []
    ind=0
    for eachDossierLegislatif in dossierLegislatifList:
        ind+=1
        if ind < 60000:
            #print("eachDossierLegislatif :", eachDossierLegislatif)
            for eachVote in eachDossierLegislatif.get("votesList"):
                #print("\t -> ", "ind : ", ind, " - voteRef : ", eachVote.get("voteRef"))
                if eachVote.get("voteRef") != "VTANR5L15V4689": # ce vote n'est pas dans la BDD scrutin :/ 
                    dossierLegislatifVoteLinks.append({
                        'dossierLegislatifID': eachDossierLegislatif.get("uid"),
                        'voteID': eachVote.get("voteRef"),
                        })
        if ind == 6 and False:
            print("dossierLegislatifVoteLinks : ", dossierLegislatifVoteLinks)
    
    print("\n ** Récupération FIN **\n")
    return dossierLegislatifVoteLinks


def getVoteDetails(dossierLegislatifVoteLinks):
    print("\n\n ** Récupération des détails de chaque vote **\n")
    voteOK=0
    voteNOK=0
    with open(pathdata+'scrutinList.json') as json_data:
        scrutinList = json.load(json_data)
    
    for eachDossier in dossierLegislatifVoteLinks:
        voteID=eachDossier.get("voteID")
        #print("scrutinList len : ", len(scrutinList))
        voteOKBool=False
        for eachVoteDetail in scrutinList:
            if voteID == eachVoteDetail.get("uid"):
                #print("\t -> OK")
                eachDossier['voteDetails'] = eachVoteDetail
                voteOKBool=True
        if voteOKBool:
            voteOK+=1
        else:
            voteNOK+=1
            print("voteID NOK : ", voteID )
    
    print("\ndossierLegislatifVoteLinks :", dossierLegislatifVoteLinks[0:2])
    print("\nvoteOK :", voteOK, " / ", len(dossierLegislatifVoteLinks))
    print("\nvoteNOK :", voteNOK, " / ", len(dossierLegislatifVoteLinks))
    print("\n ** Récupération FIN **\n")
    return dossierLegislatifVoteLinks
    
    
def storeOrganeDossierlegislatifVoteLinksInNeo(dossierLegislatifVoteLinks):
    print("\n\n ** CREATION DES LIENS ORGANE-[vote]->DOSSIERLEG DANS NEO4J **\n")
    t = time.time()
    printDev = True
    indDossier=0
    indAll=0
    for eachDossier in dossierLegislatifVoteLinks:
        indDossier+=1
        if indDossier <= 300000:
            dossierLegislatifID=eachDossier.get('dossierLegislatifID')
            indGroup=0
            for eachGroup in eachDossier.get('voteDetails').get('groupVotes'):
                indGroup+=1
                indAll+=1
                groupID=eachGroup.get('organeRef')
                printDev=False
                if printDev:
                    print("\nDossier : ", indDossier, " - Groupe : ", indGroup, " - Iteration : ", indAll)
                if False:
                    print("dossierLegislatifID : ", dossierLegislatifID)
                    print("groupID : ", groupID)
                query = "match(o:Organe{uid:'"+groupID+"'}) ,(dl:DossierLegislatif{uid:'"+dossierLegislatifID+"'}) merge (o)-[l:"+"VOTEDFOR"+"{positionMajoritaire: '"+eachGroup.get('positionMajoritaire')+"', decompteVoix_pour: '"+ str(eachGroup.get('decompteVoix_pour'))+"' }] -> (dl) ;"
                session.run(query=query)
                if printDev: print("\t -> Création de la relation  ", groupID, "-> ", dossierLegislatifID," OK")
    elapsed = time.time() - t
    print("\nFin des relations acteurs --> organes")
    print("\nNombre de relations créées : ", indAll, " en ", round(elapsed, 2), " s")
    
    
    
    
def storeActeurDossierlegislatifVoteLinksInNeo(dossierLegislatifVoteLinks):
    print("\n\n ** CREATION DES LIENS ACTEUR-[vote]->DOSSIERLEG DANS NEO4J **\n")
    t = time.time()
    printDev = True
    indDossier=0
    indAll=0
    for eachDossier in dossierLegislatifVoteLinks:
        indDossier+=1
        if indDossier <= 300000:
            dossierLegislatifID=eachDossier.get('dossierLegislatifID')
            indGroup=0
            for eachGroup in eachDossier.get('voteDetails').get('actorVotes'):
                indGroup+=1
                indAll+=1
                groupID=eachGroup.get('acteurRef')
                printDev=False
                if indAll%250==0:
                    print("\nDossier : ", indDossier, " - Acteur : ", indGroup, " - Iteration : ", indAll)
                if printDev:
                    print("dossierLegislatifID : ", dossierLegislatifID)
                    print("groupID : ", groupID)
                query = "match(a:Acteur{uid:'"+groupID+"'}) ,(dl:DossierLegislatif{uid:'"+dossierLegislatifID+"'}) merge (a)-[l:"+"VOTEDFOR"+"{positionVote: '"+eachGroup.get('positionVote')+"', decompteVoix_pour: '"+ str(eachGroup.get('decompteVoix_pour'))+"' }] -> (dl) ;"
                session.run(query=query)
                if printDev: print("\t -> Création de la relation  ", groupID, "-> ", dossierLegislatifID," OK")            
    elapsed = time.time() - t    
    print("\nFin des relations acteurs --> organes")
    print("\nNombre de relations créées : ", indAll, " en ", round(elapsed, 2), " s")
    

    
if False: storeDossiersLegislatifsInNeo()

def lancheringestion():   
    dossierLegislatifVoteLinks = getDossierlegVoteLinks()
    dossierLegislatifVoteLinks = getVoteDetails(dossierLegislatifVoteLinks)
    storeOrganeDossierlegislatifVoteLinksInNeo(dossierLegislatifVoteLinks)
    storeActeurDossierlegislatifVoteLinksInNeo(dossierLegislatifVoteLinks)
#print("\nFIN")


