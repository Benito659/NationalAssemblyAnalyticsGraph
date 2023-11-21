from fileinput import filename
from neo4j import GraphDatabase
from posixpath import basename
import json
import os
from typing import Set

data_base_connection = GraphDatabase.driver(uri = "bolt://neo4j:7687", auth=("neo4j", "123bendageekW@"))
session = data_base_connection.session()   
pathOrgane = "./data/organe" #Chemin d'acces vers les fichiers json des organes
pathActeur = "./data/acteur" #Chemin d'acces vers les fichiers json des deputés
selectedOrganeType = ["API","ASSEMBLEE","CMP","CNPE","CNPS","COMNL","COMPER","COMSENAT","COMSPSENAT","CONFPT","CONSTITU","GA","GOUVERNEMENT","GP","GROUPESENAT","MINISTERE","ORGAINT","ORGEXTPARL","PARPOL","PRESREP","SENAT"]
selectedActorQualite = {
    "Vice-président": "Vice_président","Secrétaire": "Secrétaire","Rapporteur général": "Rapporteur", "Membre": "Membre",
    "Président délégué": "Président", "Président": "Président", "Député non-inscrit": "Député_non_inscrit",  "Ministre d'État":"Ministre_État", "ministre": "Ministre",
    "membre": "Membre", "Secrétaire général-adjoint": "Secrétaire",  "Président exécutif": "Président", "Membre du": "Membre",
    "Membre du Bureau": "Membre",  "Président du": "Président",  "Membre titulaire": "Membre",  "Ministre": "Ministre",
    "Président de droit": "Président", "Rapporteur thématique": "Rapporteur",  "Membre suppléant": "Membre",  "en mission": "en mission",
    "Secrétaire d'État": "Secrétaire d'État",  "Rapporteur": "Rapporteur",   "Vice-Président": "Vice_président"
}
actorsList = []
organeList = []
actorOrganLinkList = []
selectedOrganIdList = []  #variable pour contenir les id des  organes issues de la limitation (#deputées et numbre de types voulus)
#sample limitation variables 
deputeeNumberLimit = 50
organTypeNumberLimit = 5
totalOrganescreated = 0

def nameRelation(name):
    #- une propriété commence toujours par une lettre minuscule
    #- une relation est tout en majuscul  
    pass   

def storeOrganesInNeo():   
    
    printDev = True
    if printDev: print("\n\n\nCREATION DES NOEUDS 'ORGAN' DANS NEO4J :\n")
    
    #add primary
    try:
        query_unique_constraint = "CREATE CONSTRAINT ON (organe:Organe) ASSERT organe.uid IS UNIQUE"
        query_exists_constraint = "CREATE CONSTRAINT ON (organe:Organe) ASSERT exists(organe.uid)"
        session.run(query=query_unique_constraint)
        session.run(query=query_exists_constraint)
    except :
        print('Already created constraint') 

    for organe in organeList:        
        #create organes
        query = "merge (o:Organe{uid:'"+ str(organe['uid']) +"' , codeType:'"+ str(organe['codeType']) +"' , libelle:\""+ str(organe['libelle']) +"\", dateDebut : '"+ str(organe['dateDebut']) + "', dateFin : '"+ str(organe['dateFin']) + "'})"
        session.run(query=query)
        if printDev: print("\t -> Création du noeud ", organe['uid'], " OK")
          
    if printDev: print("Fin de la création des noeuds organes")
    
def storeActorsInNeo():   
    
    printDev = True
    if printDev: print("\n\n\nCREATION DES NOEUDS 'ACTEURS' DANS NEO4J :\n")
    
    #add primary
    try:
        query_unique_constraint = "CREATE CONSTRAINT ON (acteur:Acteur) ASSERT acteur.uid IS UNIQUE"
        query_exists_constraint = "CREATE CONSTRAINT ON (acteur:Acteur) ASSERT exists(acteur.uid)"
        session.run(query=query_unique_constraint)
        session.run(query=query_exists_constraint)
    except :
        print('Already created constraint') 

    for acteur in actorsList:        
        #create actors node
        query = "merge (a:Acteur{uid:'"+ str(acteur['uid']) +"' , civ:'"+ str(acteur['civ']) +"' , prenom: \""+ str(acteur['prenom']) +"\", nom : \""+ str(acteur['nom']) + "\", dateNais : '"+ str(acteur['dateNais']) +"' , villeNais:\""+ str(acteur['villeNais']) +"\" , paysNais:\""+ str(acteur['paysNais']) +"\", dateDeces : '"+ str(acteur['dateDeces']) + "', profession : \""+ str(acteur['profession']) + "\"})"
        session.run(query=query)
        if printDev: print("\t -> Création du noeud ", acteur['uid'], " OK")
        
    if printDev: print("Fin de la création des noeuds acteur")


def storeActorOrganLinksInNeo():
    printDev = True
    for link in actorOrganLinkList:
        linkName = selectedActorQualite.get(link["infosQualite"])
        if linkName:
            query = "match(a:Acteur{uid:'"+str(link["acteurRef"])+"'}) ,(o:Organe{uid:'"+str(link["organeRef"])+"'}) create (a)-[l:"+str(linkName)+"{dateDebut: '"+str(link["dateDebut"])+"', dateFin: '"+ str(link["dateFin"])+"', legislature: '"+str(link["legislature"])+"',infosQualiteRegion: \""+str(link["infosQualiteRegion"])+"\",infosQualiteDepartement:\""+ str(link["infosQualiteDepartement"])+"\",causeMandat:\""+ str(link["causeMandat"])+"\" }] -> (o)"
            session.run(query=query)
            if printDev: print("\t -> Création de la relation  ", link["acteurRef"], "-> ", link["organeRef"]," OK")
    if printDev: print("Fin des rélations acteurs --> organes")

def getOrganeProperties(fileName): #methode pour lire le fichier json
    with open(fileName) as file:
        data = json.load(file)
        # if data['organe'].get("uid") in selectedOrganIdList:
        organeTampon = {
            "uid": data['organe'].get("uid"),
            "codeType": data['organe'].get("codeType"),
            "libelle": data['organe'].get("libelle"),
            "dateDebut": data['organe'].get("viMoDe").get("dateDebut",""),
            "dateFin": data['organe'].get("viMoDe").get("dateFin") if(data['organe'].get("viMoDe").get("dateFin")) else None
        }
        organeList.append(organeTampon)
                    

def getAllOrgansJsonFiles(basepath) : #methode pour detecter automatiquement tous les fichiers csv dans le reportoire passé en parametre
    global selectedOrganIdList
    totalNumber = 0  
    extractionOkNumber = 0
    
    printDev = True
    if printDev: print("\n\nRECUPERATION DES DONNEES DE CHAQUE ORGANE :")
    
    for entry in os.listdir(basepath):
        totalNumber+=1
        if os.path.isfile(os.path.join(basepath, entry)):
            
            # Récupération du nom et de l'extension du fichier
            fileNameWithoutExtension = os.path.splitext(entry)[0]
            extension = os.path.splitext(entry)[1]
            
            # Si le fichier (fileNameWithoutExtension) est bien dans la liste des organes à récupérer, alors on l'ajoute dans Neo4J
            if extension == ".json" and fileNameWithoutExtension in selectedOrganIdList:    
                extractionOkNumber += 1
                getOrganeProperties(os.path.join(basepath, entry))                
                # if printDev: print("\tSUCCESSFULLY EXTRACTED DATA FROM "+ entry)
      
    
    if extractionOkNumber<len(selectedOrganIdList):
        print("\n\n !!!!! WARNING !!!!! \n Some organs have not been imported !!!! \n\n")
    else: 
        if printDev: print(" -> Extraction OK : \n\t -> number of extracted organs : ", extractionOkNumber, " among ", len(selectedOrganIdList), " expected \n\t -> organs list : ", organeList)
        

def getUniqueOrganIdList():
    global actorOrganLinkList
    global selectedOrganIdList
    
    printDev = False
    if printDev: print("\nRECUPERATION DES ID UNIQUES DES ORGANES :")
    
    for eachLink in actorOrganLinkList:
        if eachLink['organeRef'] not in selectedOrganIdList:
            selectedOrganIdList.append(eachLink['organeRef'])
    if printDev: print("\nlen getUniqueOrganIdList : ", len(selectedOrganIdList))
    if printDev: print("getUniqueOrganIdList : ", selectedOrganIdList)


def getAndStoreOrganes(basepath):
   
    getUniqueOrganIdList()
    getAllOrgansJsonFiles(basepath)  
    storeOrganesInNeo()   
    storeActorsInNeo()   #!!!!!!! Ajout des acteurs dans Neo à intégrer à partir de actorsList
    storeActorOrganLinksInNeo() #  !!!!!!! Ajout des liens Acteurs -> Organes dans Neo à intégrer à partir de actorOrganLinkList
        

def IdentifyRequiredOrganes(basepath):
    global actorOrganLinkList
    counter = 1
    
    printDev = True
    if printDev: print("\nRECUPARATION DES ORGANES PERTINENTS ET DES LIENS ACTEURS -> ORGANES\n")
    
    for entry in os.listdir(basepath):
        if os.path.isfile(os.path.join(basepath, entry)):
            extension = os.path.splitext(entry)[1]            
            if extension == ".json":
                if counter <=deputeeNumberLimit: 

                    filename = os.path.join(basepath, entry)
                    print("filename : ", filename)
                    
                    
                    
                    with open(filename) as file:
                        data = json.load(file)
                        
                        # Récupération des données des Acteurs
                        
                        actorsList.append({
                                'uid': data['acteur'].get("uid").get("#text"), 
                                'civ': data['acteur'].get("etatCivil").get("ident").get("civ"), 
                                'prenom': data['acteur'].get("etatCivil").get("ident").get("prenom"), 
                                'nom': data['acteur'].get("etatCivil").get("ident").get("nom"), 
                                'dateNais': data['acteur'].get("etatCivil").get("infoNaissance").get("dateNais"), 
                                'villeNais': data['acteur'].get("etatCivil").get("infoNaissance").get("villeNais"),
                                'paysNais': data['acteur'].get("etatCivil").get("infoNaissance").get("paysNais"),
                                'dateDeces': data['acteur'].get("etatCivil").get("dateDeces"),
                                'profession': data['acteur'].get("profession").get("libelleCourant"),
                            })
                        
                        
                        # Récupération des données des liens Acteurs -> Organes
                        allMandats =  data['acteur'].get("mandats").get("mandat")
                        
                        localActorOrganLinkList=[]
                        for mandat in allMandats:
                            
                            # Si le type d'organe fait partie de la short list + limitation du nombre d'organes à récupérer (organTypeNumberLimit)
                            
                            #!!!!!!Ajouter un filtre sur les qualités du mandat/relation !!!!!

                            if mandat.get("typeOrgane") in selectedOrganeType and len(localActorOrganLinkList)<organTypeNumberLimit: #on prends les organes qui appartiennent à la liste reduite de type d'organes
                                localActorOrganLinkList.append(
                                    {
                                        'acteurRef': mandat.get("acteurRef"), 
                                        'organeRef': mandat.get("organes").get("organeRef"),
                                        'typeOrgane': mandat.get("typeOrgane"), 
                                        'dateDebut': mandat.get("dateDebut"), 
                                        'dateFin': mandat.get("dateFin",""), 
                                        'legislature': mandat.get("legislature"), 
                                        'infosQualite': mandat.get("infosQualite").get("codeQualite"),
                                        'infosQualiteRegion': mandat.get("election").get("lieu").get("region") if ("election" in mandat and "lieu" in mandat.get("election") and "region" in mandat.get("election").get("lieu")) else None,
                                        'infosQualiteDepartement': mandat.get("election").get("lieu").get("departement") if ("election" in mandat and "lieu" in mandat.get("election") and "departement" in mandat.get("election").get("lieu")) else None,
                                        'causeMandat': mandat.get("election").get("causeMandat") if ("election" in mandat and "causeMandat" in mandat.get("election") ) else None,
                                    })
                        
                        if len(localActorOrganLinkList)>0: 
                            actorOrganLinkList+=localActorOrganLinkList
                            
                    counter +=1
    if printDev: print(" -> Récupération OK :")
    if printDev: print("\n\t -> Nombre d'acteurs récupérés : ", len(actorsList))
    if printDev: print("\t -> Détail acteurs : ", actorsList)
    if printDev: print("\n\t -> Nombre de liens Acteur -> Organe récupérés : ", len(actorOrganLinkList))
    if printDev: print("\t -> Détail actorOrganLinkList : ", actorOrganLinkList)



if __name__ == "__main__":    
    print("\n=== PROJET NEO4J - IMPORT SAMPLE ORGANES, ACTORS AND RELATIONSHIPS IN NEO4J===\n")  
    
             #1- IDENTIFY THE ORGANS CORRESPONDING TO THE n FIRST DEPUTEES AND m FIRST ORGANE TYPES
    IdentifyRequiredOrganes(pathActeur)
    
            #2- GET AND STORE THE IDENTIFIED ORGANS
    getAndStoreOrganes(pathOrgane)
    #print(len(organeList))

            #3-CREATE FIRST n DEPUTEES NODES AND CREATE CORRESPONDING RELATIONSHIPS

    
    session.close()   
    print("\n=== PROJET NEO4J - IMPORT SAMPLE ORGANES, ACTORS AND RELATIONSHIPS IN NEO4J ===\n")
    





















