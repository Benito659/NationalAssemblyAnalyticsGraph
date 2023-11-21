from neo4j import GraphDatabase
import json
import os
import time

import importJSON 

    
# data_base_connection = GraphDatabase.driver(uri = "bolt://neo4j:7687", auth=("neo4j", "123narutoC."))

import os
db_host = os.environ.get('NEO4J_HOST', 'localhost')
db_port = os.environ.get('NEO4J_PORT', '7687')
data_base_connection = GraphDatabase.driver(uri='bolt://project-neo4jplus-1:7687',auth=("neo4j", "123narutoC."))
print("db_host == ",db_host,"db_port == ",db_port)

session = data_base_connection.session()


pathActeur = "./data/acteur"
pathAmendement ="./data/amendement/amendementList.json"
pathDossier ="./data/dossierLegislatif/dossierLegislatifList.json"
selectedOrganeType = ["API","ASSEMBLEE","CMP","CNPE","CNPS","COMNL","COMPER","COMSENAT","COMSPSENAT","CONFPT","CONSTITU","GA","GOUVERNEMENT","GP","GROUPESENAT","MINISTERE","ORGAINT","ORGEXTPARL","PARPOL","PRESREP","SENAT"]

selectedActorQualite = {
    "Vice-président": "Vice_président","Secrétaire": "Secrétaire","Rapporteur général": "Rapporteur", "Membre": "Membre",
    "Président délégué": "Président", "Président": "Président", "Député non-inscrit": "Député_non_inscrit",  "Ministre d'État":"Ministre_État", "ministre": "Ministre",
    "membre": "Membre", "Secrétaire général-adjoint": "Secrétaire",  "Président exécutif": "Président", "Membre du": "Membre",
    "Membre du Bureau": "Membre",  "Président du": "Président",  "Membre titulaire": "Membre",  "Ministre": "Ministre",
    "Président de droit": "Président", "Rapporteur thématique": "Rapporteur",  "Membre suppléant": "Membre",  "en mission": "en mission",
    "Secrétaire d'État": "Secrétaire d'État",  "Rapporteur": "Rapporteur",   "Vice-Président": "Vice_président"
}


pathOrgane = "./data/organe"
organTypeNumberLimit = 5
organeList = []
actorOrganLinkList = []
selectedOrganIdList = []  #variable pour contenir les id des  organes issues de la limitation (#deputées et numbre de types voulus)
totalOrganescreated = 0  
selectedOrganeType = ["API","ASSEMBLEE","CMP","CNPE","CNPS","COMNL","COMPER","COMSENAT","COMSPSENAT","CONFPT","CONSTITU","GA","GOUVERNEMENT","GP","GROUPESENAT","MINISTERE","ORGAINT","ORGEXTPARL","PARPOL","PRESREP","SENAT"]
actorsList = []
deputeeNumberLimit = 10000

# Paramètres dossier législatif
dossierLegislatifPath = "./data/dossierLegislatif"
dossierLegislatifLimit=4
dossierLegislatifList=[]

# Pamaètres textes de loi
texteDeLoiPath = "./data/texteDeLoi"
texteDeLoiLimit=4
texteDeLoiList=[]

# Paramètres scrutins
scrutinPath = "./data/scrutin"
scrutinLimit=4
scrutinList=[]

# Paramètres amendements
amendementPath = "./data/json"
amendementList=[]
amendementList=[]
#relation: soumet
#relation : vote & amendement
actorAndAmendementLinkedList=[]
#lien amendement/list
dossierAndAmendementLinkedList=[]
dossierList=[]
texteDeLoiLinkAmendement=[]


def nameRelation(name):
    #- une propriété commence toujours par une lettre minuscule
    #- une relation est tout en majuscul  
    pass   



def cleanLibelle(libelle):
    return libelle.replace('"',"'")


    
def storeOrganesInNeo():   
    global organeList
    printDev = True
    if printDev: print("\n\n\nCREATION DES NOEUDS 'ORGAN' DANS NEO4J :\n")
    # if printDev: print("\norganeList :", organeList, "\n")
    try:
        query_unique_constraint = "CREATE CONSTRAINT ON (organe:Organe) ASSERT organe.uid IS UNIQUE"
        query_exists_constraint = "CREATE CONSTRAINT ON (organe:Organe) ASSERT exists(organe.uid)"
        session.run(query=query_unique_constraint)
        session.run(query=query_exists_constraint)
    except :
        print('Already created constraint') 

    for organe in organeList:        
        query = "merge (o:Organe{uid:'"+ str(organe['uid']) +"' , codeType:'"+ str(organe['codeType']) +"' , libelle:\""+ cleanLibelle(str(organe['libelle'])) +"\", dateDebut : '"+ str(organe['dateDebut']) + "', dateFin : '"+ str(organe['dateFin']) + "'});"
        session.run(query=query)
        if printDev: print("\t -> Création du noeud ", organe['uid'], " OK")
    if printDev: print("Fin de la création des noeuds organes")
    
def storeActorsInNeo():   
    printDev = True
    if printDev: print("\n\n\nCREATION DES NOEUDS 'ACTEURS' DANS NEO4J :\n")
    try:
        query_unique_constraint = "CREATE CONSTRAINT ON (acteur:Acteur) ASSERT acteur.uid IS UNIQUE"
        query_exists_constraint = "CREATE CONSTRAINT ON (acteur:Acteur) ASSERT exists(acteur.uid)"
        session.run(query=query_unique_constraint)
        session.run(query=query_exists_constraint)
    except :
        print('Already created constraint') 
    
    for acteur in actorsList:        
        try:
            query = "merge (a:Acteur{uid:'"+ str(acteur['uid']) +"' , civ:'"+ str(acteur['civ']) +"' , prenom: \""+ str(acteur['prenom']) +"\", nom : \""+ str(acteur['nom']) + "\", dateNais : '"+ str(acteur['dateNais']) +"' , villeNais:\""+ str(acteur['villeNais']) +"\" , paysNais:\""+ str(acteur['paysNais']) +"\", dateDeces : '"+ str(acteur['dateDeces']) + "', profession : \""+ str(acteur['profession']) + "\"});"
            session.run(query=query)
            if printDev: print("\t -> Création du noeud ", acteur['uid'], " OK")
        except:
            print("don't work")
    if printDev: print("Fin de la création des noeuds acteur")


def storeActorOrganLinksInNeo():
    printDev = True
    for link in actorOrganLinkList:
        linkName = selectedActorQualite.get(link["infosQualite"])
        if linkName and type(link["organeRef"]) == type(''):
            query = "match(a:Acteur{uid:'"+str(link["acteurRef"])+"'}) ,(o:Organe{uid:'"+(str(link["organeRef"]) )+"'}) merge (a)-[l:"+str(linkName).replace(" ","_").replace("'","")+"{dateDebut: '"+str(link["dateDebut"])+"', dateFin: '"+ str(link["dateFin"])+"', legislature: '"+str(link["legislature"])+"',infosQualiteRegion: \""+str(link["infosQualiteRegion"])+"\",infosQualiteDepartement:\""+ str(link["infosQualiteDepartement"])+"\",causeMandat:\""+ str(link["causeMandat"])+"\" }] -> (o) ;"
            session.run(query=query)
            try:
                secondquery="match(a:Acteur{uid:'"+str(link["acteurRef"])+"'}) ,(o:Organe{uid:'"+(str(link["organeRef"]) )+"'}) merge (a)-[l:ISPARTIPOLITIQUE{ispart: True}] -> (o) ;"
                session.run(query=secondquery)
            except:
                pass
            if printDev: print("\t -> Création de la relation  ", link["acteurRef"], "-> ", link["organeRef"]," OK")
    if printDev: print("Fin des relations acteurs --> organes")


def getOrganeProperties(fileName): 
    with open(fileName) as file:
        data = json.load(file)
        organeTampon = {
            "uid": data['organe'].get("uid"),
            "codeType": data['organe'].get("codeType"),
            "libelle": data['organe'].get("libelle"),
            "dateDebut": data['organe'].get("viMoDe").get("dateDebut",""),
            "dateFin": data['organe'].get("viMoDe").get("dateFin") if(data['organe'].get("viMoDe").get("dateFin")) else None
        }
        print("\ngetOrganeProperties : ", organeTampon)
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
            fileNameWithoutExtension = os.path.splitext(entry)[0]
            extension = os.path.splitext(entry)[1]
            print("\nfileNameWithoutExtension : ", fileNameWithoutExtension)
            print("selectedOrganIdList : ", selectedOrganIdList)
            print("extension : ", extension)
            # Si le fichier (fileNameWithoutExtension) est bien dans la liste des organes à récupérer, alors on l'ajoute dans Neo4J
            if extension == ".json" and fileNameWithoutExtension in selectedOrganIdList:    
                extractionOkNumber += 1
                getOrganeProperties(os.path.join(basepath, entry))                
                if printDev: print("\tSUCCESSFULLY EXTRACTED DATA FROM "+ entry)
    
    if extractionOkNumber<len(selectedOrganIdList):
        print("\n\n !!!!! WARNING !!!!! \n Some organs have not been imported !!!! \n\n")
    if printDev: print(" -> Extraction OK : \n\t -> number of extracted organs : ", extractionOkNumber, " among ", len(selectedOrganIdList), " expected \n\t -> organs list : ", organeList)
        

def getUniqueOrganIdList():
    global actorOrganLinkList
    global selectedOrganIdList
    printDev = True
    if printDev: print("\nRECUPERATION DES ID UNIQUES DES ORGANES :")
    for eachLink in actorOrganLinkList:
        if eachLink['organeRef'] not in selectedOrganIdList:
            selectedOrganIdList.append(eachLink['organeRef'])
    if printDev: print("\nlen getUniqueOrganIdList : ", len(selectedOrganIdList))
    if printDev: print("getUniqueOrganIdList : ", selectedOrganIdList)


def getAndStoreOrganes(basepath):
    storeActorsInNeo()   #!!!!!!! Ajout des acteurs dans Neo à intégrer à partir de actorsList
    getUniqueOrganIdList()  # 3. Récupère les id (uniques) des organes qui figurent dans listeMandats => selectedOrganIdList
    getAllOrgansJsonFiles(basepath)  
    storeOrganesInNeo()   
    #storeActorsInNeo()   #!!!!!!! Ajout des acteurs dans Neo à intégrer à partir de actorsList
    storeActorOrganLinksInNeo() #  !!!!!!! Ajout des liens Acteurs -> Organes dans Neo à intégrer à partir de actorOrganLinkList
        

def getActorsAndOrgans(basepath):
    global actorOrganLinkList
    counter = 0
    printDev = True
    if printDev: print("\nRECUPARATION DES ORGANES PERTINENTS ET DES LIENS ACTEURS -> ORGANES\n")
    for entry in os.listdir(basepath):
        if os.path.isfile(os.path.join(basepath, entry)):
            extension = os.path.splitext(entry)[1]            
            if extension == ".json":
                if counter <=deputeeNumberLimit: 
                    filename = os.path.join(basepath, entry)
                    with open(filename) as file:
                        data = json.load(file)
                        # Récupération des données des Acteurs
                        dateDeces = data['acteur'].get("etatCivil").get("dateDeces")
                        paysNais = data['acteur'].get("etatCivil").get("infoNaissance").get("paysNais")
                        actorsList.append({
                                'uid': data['acteur'].get("uid").get("#text"), 
                                'civ': data['acteur'].get("etatCivil").get("ident").get("civ"), 
                                'prenom': data['acteur'].get("etatCivil").get("ident").get("prenom"), 
                                'nom': data['acteur'].get("etatCivil").get("ident").get("nom"), 
                                'dateNais': data['acteur'].get("etatCivil").get("infoNaissance").get("dateNais"), 
                                'villeNais': data['acteur'].get("etatCivil").get("infoNaissance").get("villeNais"),
                                'paysNais': paysNais if (type(paysNais)!=type({})) else None,
                                'dateDeces':dateDeces if (len(dateDeces)==10) else None,
                                'profession': data['acteur'].get("profession").get("libelleCourant"),
                            })
                        # Récupération des données des liens Acteurs -> Organes
                        allMandats =  data['acteur'].get("mandats").get("mandat")
                        localActorOrganLinkList=[]
                        for mandat in allMandats:
                            # Si le type d'organe fait partie de la short list + limitation du nombre d'organes à récupérer (organTypeNumberLimit)
                            #!!!!!!Ajouter un filtre sur les qualités du mandat/relation !!!!!
                            if type(mandat) != type('str'):
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
    # if printDev: print("\n\t -> Nombre d'acteurs récupérés : ", len(actorsList))
    # if printDev: print("\t -> Détail acteurs : ", actorsList)
    # if printDev: print("\n\t -> Nombre de liens Acteur -> Organe récupérés : ", len(actorOrganLinkList))
    # if printDev: print("\t -> Détail actorOrganLinkList : ", actorOrganLinkList)


def getDossierLegislatifData(basepath):
    global dossierLegislatifList
    counter = 0
    nbVotes=0
    countVTAN=0
    votesListAll=[]
    printDev = True
    if printDev: print("\nRECUPERATION DES DONNEES DOSSIER LEGISLATIF\n")
    for entry in os.listdir(basepath):
        counter += 1
        if os.path.isfile(os.path.join(basepath, entry)):
            extension = os.path.splitext(entry)[1]            
            if extension == ".json":
                if counter <= len(os.listdir(basepath))+10: # dossierLegislatifLimit: 
                    filename = os.path.join(basepath, entry)
                    print("\n\nfilename", counter, "/", len(os.listdir(basepath)), " : ", filename)
                    with open(filename) as file:
                        data = json.load(file)
                        
                        # Récupération des données des dossiers parlementaires
                        includesVTAN="VTAN" in json.dumps(data)
                        if includesVTAN:
                            countVTAN+=json.dumps(data).count("VTAN")
                        # print("VTAN ? : ", includesVTAN)
                        
                        # Récupération des initiateurs du DP
                        initiateursList=[]
                        if data['dossierParlementaire'].get("initiateur") is not None and "acteurs" in data['dossierParlementaire'].get("initiateur"):
                            acteursData=data['dossierParlementaire'].get("initiateur").get("acteurs").get("acteur")
                            isList=isinstance(acteursData, list)
                            if isList:
                                initiateursList=[x.get("acteurRef") for x in acteursData]
                            else:
                                initiateursList=[acteursData.get("acteurRef")]
                        
        
                        
                        # Récupération des votes du DP
                        if data['dossierParlementaire'].get("actesLegislatifs") is not None:
                            tempDossierLegislatifNiv1=data['dossierParlementaire'].get("actesLegislatifs").get("acteLegislatif")
                            
                            # data['dossierParlementaire'].get("actesLegislatifs").get("acteLegislatif")[1].get("actesLegislatifs").get("acteLegislatif")[-1].get("actesLegislatifs").get("acteLegislatif")[-1]
                            # data['dossierParlementaire'].get("actesLegislatifs").get("acteLegislatif")[i].get("actesLegislatifs").get("acteLegislatif")[-1].get("actesLegislatifs").get("actesLegislatif")
                            
                            # print("\t -> tempDossierLegislatifNiv1 : ", tempDossierLegislatifNiv1)
                            
                            votesList=[]
                            actesLegislatifsNiv1=[]
                            actesLegislatifsNiv2=[]
                            actesLegislatifsNiv3=[]
                            if not isinstance(tempDossierLegislatifNiv1, list):
                                tempDossierLegislatifNiv1=[tempDossierLegislatifNiv1]
                                
                            indTemp=0
                            checkBool=False
                            for eachDossierNiv1 in tempDossierLegislatifNiv1:
                                vote=None
                                texteDeLoi=None
                                tempDossierLegislatifNiv2=None
                                tempDossierLegislatifNiv3=None
                                
                                
                                tempDossierLegislatifNiv2=eachDossierNiv1.get("actesLegislatifs").get("acteLegislatif")
                                
                                
                                if not isinstance(tempDossierLegislatifNiv2, list):
                                    tempDossierLegislatifNiv2=[tempDossierLegislatifNiv2]
                                
                                for eachDossierNiv2 in tempDossierLegislatifNiv2:
                                    
                                    if eachDossierNiv2.get("actesLegislatifs") is not None:
                                            tempDossierLegislatifNiv3=eachDossierNiv2.get("actesLegislatifs").get("acteLegislatif")
    
                                    actesLegislatifsNiv2.append({
                                        'uid': eachDossierNiv2.get("uid"),
                                        'codeActe': eachDossierNiv2.get("codeActe"),
                                        'organeRef': eachDossierNiv2.get("organeRef"),
                                        'dateActe': eachDossierNiv2.get("dateActe"),
                                        'nomCanonique': eachDossierNiv2.get("libelleActe").get("nomCanonique"),
                                        'libelleCourt': eachDossierNiv2.get("libelleActe").get("libelleCourt"),
                                        'texteAssocie': eachDossierNiv2.get("texteAssocie"),
                                    })
                                    
                                    if not isinstance(tempDossierLegislatifNiv3, list):
                                        tempDossierLegislatifNiv3=[tempDossierLegislatifNiv3]
                                    
                                    for eachDossierNiv3 in tempDossierLegislatifNiv3:
                                        if eachDossierNiv3 is not None and eachDossierNiv3.get("voteRefs") is not None:
                                            vote=eachDossierNiv3.get("voteRefs").get("voteRef")
                                            reunionRef=eachDossierNiv3.get("reunionRef")
                                            codeActe=eachDossierNiv3.get("codeActe")
                                            
                                            texteDeLoi=None
                                            if eachDossierNiv3.get("textesAssocies") is not None:
                                                if isinstance(eachDossierNiv3.get("textesAssocies").get("texteAssocie"),list):
                                                    for eachText in eachDossierNiv3.get("textesAssocies").get("texteAssocie"):
                                                        if eachText.get("typeTexte")=="BTA":
                                                            texteDeLoi=eachText.get("refTexteAssocie")
                                                else:
                                                    texteDeLoi=eachDossierNiv3.get("textesAssocies").get("texteAssocie").get("refTexteAssocie")
                                            
                                            if not isinstance(vote, list):
                                                vote=[vote]
                                            else:
                                                checkBool=True
                                            
                                            for eachVote in vote:
                                                votesList.append({
                                                    'voteRef': eachVote, 
                                                    'texteDeLoi': texteDeLoi, 
                                                    'reunionRef': reunionRef, 
                                                    'codeActe': codeActe
                                                    })
                                                votesListAll.append({
                                                    'voteRef': eachVote, 
                                                    'texteDeLoi': texteDeLoi, 
                                                    'reunionRef': reunionRef, 
                                                    'codeActe': codeActe
                                                    })
                                                nbVotes+=1
                                        
                                        if vote is not None and False:
                                            print("\n\t\t -> eachDossierNiv1 ", indTemp)
                                            print("\t\t -> vote : ", vote)
                                        
                                
                                
                                actesLegislatifsNiv1.append({
                                    'uid': eachDossierNiv1.get("uid"),
                                    'codeActe': eachDossierNiv1.get("codeActe"),
                                    'organeRef': eachDossierNiv1.get("organeRef"),
                                    'dateActe': eachDossierNiv1.get("dateActe"),
                                    'nomCanonique': eachDossierNiv1.get("libelleActe").get("nomCanonique"),
                                    'libelleCourt': eachDossierNiv1.get("libelleActe").get("libelleCourt"),
                                    'actesLegislatifsNiv2': actesLegislatifsNiv2,
                                    })
                                
                                indTemp+=1
                            #else:
                            
                            
                            ##########################################
                            ###### Recherche du dernier élément ######
                            ##########################################
                            
                            
                            # print("lastNiv3 : ", tempDossierLegislatifNiv3[-1])
                            
                            lastNiv2=None
                            lastNiv3=None
                            lastNiv4=None
                            
                            lastNiv2 = tempDossierLegislatifNiv2[-1]
                            lastNiv3 = tempDossierLegislatifNiv3[-1]
                            
                            # print("\nlastNiv2 nomCanonique : ", lastNiv2.get("libelleActe").get("nomCanonique"))
                            # if lastNiv3 is not None:
                            #     print("lastNiv3 nomCanonique : ", lastNiv3.get("libelleActe").get("nomCanonique"))
                            
                            
                            if lastNiv3 is not None and lastNiv3.get("actesLegislatifs") is not None :
                                # print("lastNiv3 actesLegislatifs : ", lastNiv3.get("actesLegislatifs"))
                                tempDossierLegislatifNiv4=lastNiv3.get("actesLegislatifs").get("acteLegislatif")
                                # print("\ntempDossierLegislatifNiv4 : ", tempDossierLegislatifNiv4 )
                                if not isinstance(tempDossierLegislatifNiv4, list):
                                    tempDossierLegislatifNiv4=[tempDossierLegislatifNiv4]
                                
                                lastNiv4 = tempDossierLegislatifNiv4[-1]
                                # print("\nlastNiv4 : ", lastNiv4)
                                # print("\nlastNiv4 nomCanonique : ", lastNiv4.get("libelleActe").get("nomCanonique"))
                            
                            if lastNiv4 is not None:
                                lastElement=lastNiv4
                                lastELementNiv="niv4"
                            elif lastNiv3 is not None:
                                lastElement=lastNiv3
                                lastELementNiv="niv3"
                            else:
                                lastElement=lastNiv2
                                lastELementNiv="niv2"
                            
                            print("\nlastElement : ", lastElement.get("libelleActe").get("nomCanonique"), " - ", lastELementNiv)
                            print("\t -> lastElement id : ", lastElement.get("uid"))
                            print("\t -> urlEcheancierLoi : ", lastElement.get("urlEcheancierLoi"))
                            print("\n")
                                
                                
                            
                            if len(votesList)<json.dumps(data).count("VTAN") :
                                print("VTAN ? : ", json.dumps(data).count("VTAN"))
                                print("votesList : ", votesList)
                                wait = input("Press enter")
                                
                            if True and len(votesList)>0:
                                includesVTAN="VTAN" in json.dumps(data)
                                print("VTAN ? : ", json.dumps(data).count("VTAN"))
                                print("votesList : ", votesList)
                            
                            
      
                            
                            if len(votesList)>0:
                                dossierLegislatifList.append({
                                        'uid': data['dossierParlementaire'].get("uid"), 
                                        'legislature': data['dossierParlementaire'].get("legislature"), 
                                        'titre': data['dossierParlementaire'].get("titreDossier").get("titre"), 
                                        'procedureParlementaire': data['dossierParlementaire'].get("procedureParlementaire").get("libelle"), 
                                        'initiateursList': initiateursList, 
                                        'votesList': votesList,
                                        'finDossier': lastElement,
                                        'actesLegislatifsNiv1': actesLegislatifsNiv1
                                    })
                        
                        #print("\t -> vote : ", data['dossierParlementaire'].get("actesLegislatifs").get("acteLegislatif")[0].get("uid").get("actesLegislatifs").get("acteLegislatif")[-1].get("actesLegislatifs").get("acteLegislatif")[-1].get("voteRefs").get("voteRef"))
    
    if False:
        print("\n\ndossierLegislatifList (", len(dossierLegislatifList), ") : ", dossierLegislatifList)
        print("\n\nvotesListAll (", len(votesListAll), ") : ", votesListAll) 
        print("\nnb total de votes : ", nbVotes)
        print("\ncountVTAN : ", countVTAN)
    


def getTextesDeLoi(basepath):
    global texteDeLoiList
    counter = 0
    printDev = True
    if printDev: print("\nRECUPARATION DES DONNEES DOSSIER LEGISLATIF\n")
    texteDeLoiLimit = 20000
    for entry in os.listdir(basepath):
        counter += 1
        if os.path.isfile(os.path.join(basepath, entry)):
            extension = os.path.splitext(entry)[1]            
            if extension == ".json":
                if counter <= texteDeLoiLimit:  

                    filename = os.path.join(basepath, entry)
                    print("\n\nfilename", counter, "/", len(os.listdir(basepath)), " : ", filename)
                    
                    with open(filename) as file:
                        data = json.load(file)
        
                        #print("aaa : ", data['document'].get("auteurs").get("auteur").get("acteur"))
                        
                        auteur=data['document'].get("auteurs").get("auteur")
                        
                        acteurList=[]
                        if not isinstance(auteur, list):
                            auteur=[auteur]
                        
                        for eachAuteur in auteur:
                            if eachAuteur.get("acteur") is not None:
                                acteur=eachAuteur.get("acteur").get("acteurRef"),
                                qualite=eachAuteur.get("acteur").get("qualite")
                                acteurList.append([acteur[0],qualite])
                        
                        texteDeLoiList.append({
                                'uid': data['document'].get("uid"), 
                                'dateCreation': data['document'].get("cycleDeVie").get("chrono").get("dateCreation"), 
                                'dateDepot': data['document'].get("cycleDeVie").get("chrono").get("dateDepot"), 
                                'datePublication': data['document'].get("cycleDeVie").get("chrono").get("datePublication"), 
                                'denominationStructurelle': data['document'].get("denominationStructurelle"), 
                                'titrePrincipal': data['document'].get("titres").get("titrePrincipal"), 
                                'titrePrincipalCourt': data['document'].get("titres").get("titrePrincipalCourt"), 
                                'dossierRef': data['document'].get("dossierRef"), 
                                'acteurRefList': acteurList, 
                                'type': data['document'].get("classification").get("type").get("libelle"), 
                                'sousType': data['document'].get("classification").get("sousType").get("libelle") if data['document'].get("classification").get("sousType") is not None else None, 
                                'familleDepot': data['document'].get("classification").get("famille").get("depot").get("libelle") if data['document'].get("classification").get("famille").get("depot") is not None else None, 
                                'familleClasse': data['document'].get("classification").get("famille").get("classe").get("libelle") if data['document'].get("classification").get("famille").get("classe") is not None else None, 
                                'familleEspece': data['document'].get("classification").get("famille").get("espece").get("libelle") if data['document'].get("classification").get("famille").get("espece") is not None else None, 
                            })
                        
    print("\n\ntexteDeLoiList (", len(texteDeLoiList), ") : ", texteDeLoiList) 
    print("\n\ntexteDeLoiList no : ", len(texteDeLoiList)) 





def getScrutins(basepath):
    global scrutinList
    counter = 0
    
    
    printDev = True
    if printDev: print("\nRECUPARATION DES SCRUTINS\n")
    
    scrutinLimit = 40000
    
    for entry in os.listdir(basepath):
        counter += 1
        if os.path.isfile(os.path.join(basepath, entry)):
            extension = os.path.splitext(entry)[1]            
            if extension == ".json":
                if counter <= scrutinLimit:  

                    filename = os.path.join(basepath, entry)
                    print("\n\nfilename", counter, "/", len(os.listdir(basepath)), " : ", filename)
                    
                    with open(filename) as file:
                        data = json.load(file)
                        
                        groupes=data['scrutin'].get("ventilationVotes").get("organe").get("groupes").get("groupe")
                        
                        groupVotes=[]
                        actorVotes=[]
                        for eachGroup in groupes:
                            groupVotes.append({
                                'organeRef': eachGroup.get("organeRef"),
                                'nombreMembresGroupe': eachGroup.get("nombreMembresGroupe"),
                                'positionMajoritaire': eachGroup.get("vote").get("positionMajoritaire"),
                                'decompteVoix_nonVotants': eachGroup.get("vote").get("decompteVoix").get("nonVotants"),
                                'decompteVoix_pour': eachGroup.get("vote").get("decompteVoix").get("pour"),
                                'decompteVoix_contre': eachGroup.get("vote").get("decompteVoix").get("contre"),
                                'decompteVoix_abstentions': eachGroup.get("vote").get("decompteVoix").get("abstentions"),
                                'decompteVoix_nonVotantsVolontaires': eachGroup.get("vote").get("decompteVoix").get("nonVotantsVolontaires"),
                                })
                            
                            nonVotants=eachGroup.get("vote").get("decompteNominatif").get("nonVotants").get("votant") if eachGroup.get("vote").get("decompteNominatif").get("nonVotants") is not None else None,
                            pours=eachGroup.get("vote").get("decompteNominatif").get("pours").get("votant") if eachGroup.get("vote").get("decompteNominatif").get("pours") is not None else None,
                            contres=eachGroup.get("vote").get("decompteNominatif").get("contres").get("votant") if eachGroup.get("vote").get("decompteNominatif").get("contres") is not None else None,
                            abstentions=eachGroup.get("vote").get("decompteNominatif").get("abstentions").get("votant") if eachGroup.get("vote").get("decompteNominatif").get("abstentions") is not None else None,
                            
                            
                            if nonVotants[0] is not None:
                                if not isinstance(nonVotants[0], list):
                                    nonVotants=([nonVotants])
                                for person in nonVotants[0]:
                                    actorVotes.append({
                                        'acteurRef': person.get("acteurRef"),
                                        'mandatRef': person.get("mandatRef"),
                                        'parDelegation': person.get("parDelegation"),
                                        'causePositionVote': person.get("causePositionVote"),
                                        'organeRef': eachGroup.get("organeRef"),
                                        'positionVote': 'nonVotant',
                                        })
                            
                            if pours[0] is not None:
                                if not isinstance(pours[0], list):
                                    pours=([pours])
                                for person in pours[0]:
                                    actorVotes.append({
                                        'acteurRef': person.get("acteurRef"),
                                        'mandatRef': person.get("mandatRef"),
                                        'parDelegation': person.get("parDelegation"),
                                        'causePositionVote': person.get("causePositionVote"),
                                        'organeRef': eachGroup.get("organeRef"),
                                        'positionVote': 'pour',
                                        })
                            
                            if contres[0] is not None:
                                if not isinstance(contres[0], list):
                                    contres=([contres])
                                for person in contres[0]:
                                    actorVotes.append({
                                        'acteurRef': person.get("acteurRef"),
                                        'mandatRef': person.get("mandatRef"),
                                        'parDelegation': person.get("parDelegation"),
                                        'causePositionVote': person.get("causePositionVote"),
                                        'organeRef': eachGroup.get("organeRef"),
                                        'positionVote': 'contre',
                                        })
                            
                            if abstentions[0] is not None:
                                if not isinstance(abstentions[0], list):
                                    abstentions=([abstentions])
                                for person in abstentions[0]:
                                    actorVotes.append({
                                        'acteurRef': person.get("acteurRef"),
                                        'mandatRef': person.get("mandatRef"),
                                        'parDelegation': person.get("parDelegation"),
                                        'causePositionVote': person.get("causePositionVote"),
                                        'organeRef': eachGroup.get("organeRef"),
                                        'positionVote': 'abstention',
                                        })
                        
                        
                        scrutinList.append({
                                'uid': data['scrutin'].get("uid"), 
                                'organeRef': data['scrutin'].get("organeRef"), 
                                'sessionRef': data['scrutin'].get("sessionRef"), 
                                'seanceRef': data['scrutin'].get("seanceRef"), 
                                'dateScrutin': data['scrutin'].get("dateScrutin"), 
                                'typeVote': data['scrutin'].get("typeVote").get("libelleTypeVote"), 
                                'titre': data['scrutin'].get("titre"), 
                                'objet': data['scrutin'].get("objet").get("libelle"), 
                                'nombreVotants': data['scrutin'].get("syntheseVote").get("nombreVotants"), 
                                'suffragesExprimes': data['scrutin'].get("syntheseVote").get("suffragesExprimes"), 
                                'nbrSuffragesRequis': data['scrutin'].get("syntheseVote").get("nbrSuffragesRequis"), 
                                'decompte_nonVotants': data['scrutin'].get("syntheseVote").get("decompte").get("nonVotants"), 
                                'decompte_pour': data['scrutin'].get("syntheseVote").get("decompte").get("pour"), 
                                'decompte_contre': data['scrutin'].get("syntheseVote").get("decompte").get("contre"), 
                                'decompte_abstentions': data['scrutin'].get("syntheseVote").get("decompte").get("abstentions"), 
                                'decompte_nonVotantsVolontaires': data['scrutin'].get("syntheseVote").get("decompte").get("nonVotantsVolontaires"), 
                                'groupVotes': groupVotes, 
                                'actorVotes': actorVotes, 
                                'uid': data['scrutin'].get("uid"), 
                            })
                        
    print("\n\nscrutinList (", len(scrutinList), ") : ", scrutinList) 
    print("\n\nscrutinList no : ", len(scrutinList)) 


def getAmendements(basepath):
    global amendementList
    counter = 0
    
    
    printDev = True
    if printDev: print("\nRECUPARATION DES AMENDEMENTS\n")
    
    dossiersLegislatifsList = os.listdir(basepath)
    #print("dossiersLegislatifsList (", len(dossiersLegislatifsList), ") : ", dossiersLegislatifsList )
    
    amendementLimit = 10000
    
    # ouverture dossier législatif
    for eachDossierLegislatif in dossiersLegislatifsList:
        counter += 1
        
        if counter <= amendementLimit:  
            
            print("\n\nDossier législatif ", counter, "/",len(dossiersLegislatifsList)," : ", eachDossierLegislatif )
            
            textesDeLoiList = os.listdir(basepath+"/"+eachDossierLegislatif)
            
            #print("\ntextesDeLoiList (", len(textesDeLoiList), ") : ", textesDeLoiList )
            
            #tempTexteDeLoiList=[]
            
            # texte de loi
            for eachTexteDeLoi in textesDeLoiList:
                #print("\n\t -> texteDeLoi : ", eachTexteDeLoi )
                
                JSONList = os.listdir(basepath+"/"+eachDossierLegislatif+"/"+eachTexteDeLoi)
                #print("\t\t -> JSONList (", len(JSONList), ") : ", JSONList )
                
                #tempJsonList=[]
                
                # json
                for eachJSON in JSONList:
                    
                    #print("\n\t -> amendement : ", eachJSON )
                    
                    with open(basepath+"/"+eachDossierLegislatif+"/"+eachTexteDeLoi+"/"+eachJSON) as file:
                        data = json.load(file)
                    
                    #print("\t\t\t -> eachJSON : ", eachJSON )
                    
                    cosignatairesList = data.get("amendement").get("signataires").get("cosignataires").get("acteurRef")
                    
                    #print("\t\t\t -> cosignatairesList : ", cosignatairesList )
                    
                    tempCosignataires=[]
                    if cosignatairesList is not None:
                        if not isinstance(cosignatairesList, list):
                            cosignatairesList=[cosignatairesList]
                        tempCosignataires=[x for x in cosignatairesList]
                    
                    amendementList.append({
                        'uid': data.get("amendement").get("uid"),
                        'dateDepot': data.get("amendement").get("cycleDeVie").get("dateDepot"),
                        'datePublication': data.get("amendement").get("cycleDeVie").get("datePublication"),
                        'dateVoteFinal': data.get("amendement").get("cycleDeVie").get("dateSort"),
                        'resultatVoteFinal': data.get("amendement").get("cycleDeVie").get("sort"),
                        'texteLegislatifRef': eachDossierLegislatif,
                        'texteDeLoi': data.get("amendement").get("texteLegislatifRef"),
                        'signataires': data.get("amendement").get("signataires").get("auteur").get("acteurRef"),
                        'cosignataires': tempCosignataires,
                        'division': data.get("amendement").get("pointeurFragmentTexte").get("division").get("titre") if data.get("amendement").get("pointeurFragmentTexte").get("division") is not None else None,
                        #'seanceDiscussionRef': data.get("amendement").get("seanceDiscussionRef"),
                        })
            
            
            #print("\t tempJsonList (", len(amendementList), ") : ", amendementList )
            
            #tempTexteDeLoiList.append(tempJsonList)
    
    print("\n\nnb total d'amendements : ", len(amendementList) )



def checkNumberOfNodes(query, text, seuil):
    
    aa = session.run(query)
    data=aa.data()
    
    if len(data)>seuil:
        print("\t -> " + text + " : " + str(len(data)) + " \t-> OK (seuil : " + str(seuil) + ")" )
        boolean=True
    else: 
        print("\t -> " + text + " : " + str(len(data)) + " \t-> Besoin de relaner l'ingestion (seuil : " + str(seuil) + ")" )
        boolean=False
        
    return boolean


def getEtatBDD():
    
    print("\nEtat de la bdd : ")
    DB_ok_OrganeBoolean = checkNumberOfNodes("MATCH (n:Organe) return n;", "Nombre d'organes", 790)
    DB_ok_ActeurBoolean = checkNumberOfNodes("MATCH (n:Acteur) return n;", "Nombre d'acteurs", 790)
    DB_ok_DossierLegislatifBoolean = checkNumberOfNodes("MATCH (n:DossierLegislatif) return n;", "Nombre de dossiers législatifs", 180)
    DB_ok_TextesDeLoiBoolean = checkNumberOfNodes("MATCH (n:Texte_de_loi) return n;", "Nombre de textes de loi", 180)
    #DB_ok_AmendementsBoolean = checkNumberOfNodes("MATCH (n:Amendement) return COUNT(n);", "Nombre d'amendements", 180)
    
    return {
        'DB_ok_OrganeBoolean': DB_ok_OrganeBoolean,
        'DB_ok_ActeurBoolean': DB_ok_ActeurBoolean,
        'DB_ok_DossierLegislatifBoolean': DB_ok_DossierLegislatifBoolean,
        }




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
                "AmandementId": amendement.get("uid"),
                "ActorId": amendement.get("signataires"),
                "CosignataireId":listCoActorInAmendement
            }

            TexteDeLoiTampon={
                "AmandementId": amendement.get("uid"),
                "texteDeLoi": amendement.get("texteDeLoi"),
            }
            amendementList.append(AmendementTampon)
            dossierAndAmendementLinkedList.append(DossierTampon)
            if(type(amendement.get("signataires"))!=type({})):
                actorAndAmendementLinkedList.append(ActorTampon)
            texteDeLoiLinkAmendement.append(TexteDeLoiTampon)
                    

def storeAmendementInNeo():   
    printDev = True
    if printDev: print("\n\n\nCREATION DES NOEUDS 'Amendement' DANS NEO4J :\n")
    
    #add primary
    try:
        query_unique_constraint = "CREATE CONSTRAINT ON (amendement:Amendement) ASSERT Amendement.uid IS UNIQUE"
        query_exists_constraint = "CREATE CONSTRAINT ON (amendement:Amendement) ASSERT exists(Amendement.uid)"
        session.run(query=query_unique_constraint)
        session.run(query=query_exists_constraint)
    except :
        print('Already created constraint') 
    
    
    t = time.time()
    elapsed = time.time() - t
    
    i=0
    for index in range(len(amendementList)) :
        i+=1      
        
        if i>75860:
            #create organes
            # try :
            query = "merge (o:Amendement{uid:'"+ str(amendementList[index]['uid']) +"' , dateDepot:'"+ str(amendementList[index]['dateDepot']) +"' , datePublication:'"+ str(amendementList[index]['datePublication']) +"', dateVoteFinal : '"+ str(amendementList[index]['dateVoteFinal']) + "', resultatVoteFinal : '"+ str(amendementList[index]['resultatVoteFinal']) + "', texteLegislatifRef : '"+str(amendementList[index]['texteLegislatifRef'])+ "', division : '"+str(cleanLibelle(amendementList[index]['division']))+"'});" 
            session.run(query=query)
            if i%1000==0: print("\t -> Création du noeud (", str(i), " / ", str(len(amendementList)), ")", amendementList[index]['uid'], " OK - Durée : ", round(time.time() - t,1), " s")
            # except:
            #     print(i)
            # if (i%10000)==0 :print(((index)/(len(amendementList)))*100)
            
    if printDev: print("Fin de la création des noeuds amendement")
    

def storeAmendementActorLinkInNeo():   
    printDev = True
    i=0
    t = time.time()
    
    for link in actorAndAmendementLinkedList:
        i+=1
        query = "match(a:Acteur{uid:'"+str(link['ActorId'])+"'}) ,(o:Amendement{uid:'"+(str(link["AmandementId"]) )+"'}) merge (a)-[l:SOUMET{cosignataires:"+str(link['CosignataireId'])+"}] -> (o) ;"
        session.run(query=query)
        if i%1000==0: print("\t -> Création de la relation  (", str(i), "/", str(len(actorAndAmendementLinkedList)), ") ", link['ActorId'], "-> ", link["AmandementId"]," OK -> ", round(time.time() - t,1), " s")
    if printDev: print("Fin des rélations acteurs --> amendements")





def getDossierProperties(fileName): #methode pour lire le fichier json
    with open(fileName) as file:
        data = json.load(file)
        # if data['organe'].get("uid") in selectedOrganIdList:
        for dossier in data : 
            DossierTampon = {
                "uid": dossier.get("uid"),
                "legislature": dossier.get("legislature"),
                "titre": dossier.get("titre"),
                "procedureParlementaire": dossier.get("procedureParlementaire")
            }
            dossierList.append(DossierTampon)


def storeDossierLegislatifInNeo():  
    printDev = True
    if printDev: print("\n\n\nCREATION DES NOEUDS 'DossierLegislatif' DANS NEO4J :\n")
    
    #add primary
    try:
        query_unique_constraint = "CREATE CONSTRAINT ON (dossierLegislatif:DossierLegislatif) ASSERT DossierLegislatif.uid IS UNIQUE"
        query_exists_constraint = "CREATE CONSTRAINT ON (dossierLegislatif:DossierLegislatif) ASSERT exists(DossierLegislatif.uid)"
        session.run(query=query_unique_constraint)
        session.run(query=query_exists_constraint)
    except :
        print('Already created constraint') 

    for dossier in dossierList:        
        #create organes
        query = "merge (o:DossierLegislatif{uid:'"+ str(dossier['uid']) +"' , legislature:'"+ str(dossier['legislature']) +"', titre : '"+ str(dossier['titre']).replace("'",'"') + "', procedureParlementaire : '"+ str(dossier['procedureParlementaire']).replace("'",'"')+"'});" 
        session.run(query=query)
        if printDev: print("\t -> Création du noeud ", dossier['uid'], " OK")
        
    if printDev: print("Fin de la création des noeuds amendement")

def storeAmendementDossierLegislatifLinkInNeo():   
    printDev = True
    for link in dossierAndAmendementLinkedList:
        query = "match(a:Amendement{uid:'"+link["AmandementId"]+"'}) ,(o:DossierLegislatif{uid:'"+(str(link["DossierId"]) )+"'}) merge (a)-[l:ISSUBMITTED] ->(o) ;"
        session.run(query=query)
        if printDev: print("\t -> Création de la relation  ",  link["AmandementId"], "-> ", link["DossierId"]," OK")
    if printDev: print("Fin des rélations amendements --> dossier")



def lancheringestion():   
    print("\n=== PROJET NEO4J - IMPORT SAMPLE ORGANES, ACTORS AND RELATIONSHIPS IN NEO4J===\n")  
    
    
    allBooleans = getEtatBDD()
    
        
    ################################################################################
    #####   1- Récupération et ingestion des données 'Acteurs' et 'Organes'    #####
    ################################################################################
    
    if not allBooleans['DB_ok_OrganeBoolean'] or not allBooleans['DB_ok_ActeurBoolean']: 
        print("cool ",allBooleans['DB_ok_OrganeBoolean']," cool 1 ",allBooleans['DB_ok_ActeurBoolean'])
        print("\n\n --------- Ingestion des acteurs et des organes dans la bdd Neo4J --------- ")
        getActorsAndOrgans(pathActeur)
        getAndStoreOrganes(pathOrgane)
    else:
        print("\n\n --------- Pas d'ingestion des acteurs et des organes dans la bdd Neo4J --------- ")
    
    
    
    #################################################################################
    #####   2 - Récupération et ingestion des données 'Dossiers législatifs'    #####
    #################################################################################
    
    if False: 
        # ---> Récupération des données 'Dossiers législatifs'
        if os.path.isfile('dossierLegislatifList.json'): # si le fichier n'existe pas dans le dossier racine
            print("\n\n --------- Pas de récupération des dossiers législatifs dans la bdd brute (le fichier 'dossierLegislatifList.json' existe déjà) --------- ")
        else:
            print("\n\n --------- Récupération des dossiers législatifs dans la bdd brute --------- ")
            getDossierLegislatifData(dossierLegislatifPath)
            
            # enregistrement du fichier 'dossierLegislatifList.json'
            with open('dossierLegislatifList.json', 'w') as f2: 
                    json.dump(dossierLegislatifList, f2)
                    print("\n\t ---> Creation du fichier 'dossierLegislatifList.json' OK --------- ")

        # ---> Ingestion des données 'Dossiers législatifs' + relations avec Acteurs et Organes
        
        if True: 
            print("\t ---> Ingestion des dossiers législatifs dans la bdd Neo4J --------- ")
            importJSON.storeDossiersLegislatifsInNeo()
        
        
        if True: 
            print("\t ---> Ingestion des dossiers législatifs dans la bdd Neo4J --------- ")
            dossierLegislatifVoteLinks = importJSON.getDossierlegVoteLinks()
            dossierLegislatifVoteLinks = importJSON.getVoteDetails(dossierLegislatifVoteLinks)
            importJSON.storeOrganeDossierlegislatifVoteLinksInNeo(dossierLegislatifVoteLinks)
            importJSON.storeActeurDossierlegislatifVoteLinksInNeo(dossierLegislatifVoteLinks)
    else:
        print("\n\n --------- Pas d'ingestion des dossiers législatifs dans la bdd Neo4J --------- ")
    
    
    
    
    #1- IDENTIFY THE ORGANS CORRESPONDING TO THE n FIRST DEPUTEES AND m FIRST ORGANE TYPES
    #IdentifyRequiredOrganes(pathActeur)
    
    #2- GET AND STORE THE IDENTIFIED ORGANS
    #getAndStoreOrganes(pathOrgane)
    #print(len(organeList))


    #3-CREATE FIRST n DEPUTEES NODES AND CREATE CORRESPONDING RELATIONSHIPS
    


    
    if False: 
        getAmendementProperties(pathAmendement)
        storeAmendementInNeo()
        storeAmendementActorLinkInNeo()

    getDossierProperties(pathDossier)
    storeDossierLegislatifInNeo()
    storeAmendementDossierLegislatifLinkInNeo()



    if False: 
        getTextesDeLoi(texteDeLoiPath)
    
        if False: 
            with open('texteDeLoiList.json', 'w') as g2:
                json.dump(texteDeLoiList, g2)
        
        
        
    if False: 
        getScrutins(scrutinPath)
    
        if False: 
            with open('scrutinList.json', 'w') as h2:
                json.dump(scrutinList, h2)
    
    
    if False:
        getAmendements(amendementPath)
        if True: 
            with open('amendementList.json', 'w') as h2:
                json.dump(amendementList, h2)
        
    session.close()   
    print("\n\n=== PROJET NEO4J - IMPORT SAMPLE ORGANES, ACTORS AND RELATIONSHIPS IN NEO4J ===\n")
    


















