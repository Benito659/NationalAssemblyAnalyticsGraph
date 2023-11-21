from neo4j import GraphDatabase
import json

# bolt://neo4j:pass@neo4j_db:7687

# data_base_connection = GraphDatabase.driver(uri = "bolt://neo4j:7687", auth=("neo4j", "123narutoC."))


import os
db_host = os.environ.get('NEO4J_HOST', 'localhost')
db_port = os.environ.get('NEO4J_PORT', '7687')
data_base_connection = GraphDatabase.driver(uri='bolt://project-neo4jplus-1:7687',auth=("neo4j", "123narutoC."))
print("db_host == ",db_host,"db_port == ",db_port)


session = data_base_connection.session()
#textDeloiFile = "./data/sampleText.json"
textDeloiFile = "./data/voteTexteDeLoiList.json" 
votesFile = "./data/scrutinList.json" #chemn d'access vers le fichier qui contient les votes
thisTexteDeLoi = {}   #get the actual text de loi
thisOrganesVotes = []
thisActeurVotes = []
scrutinsList = []

def createContraints():
    #add primary
    try:
        query_unique_constraint = "CREATE CONSTRAINT ON (texte:Texte_de_loi) ASSERT texte.uid IS UNIQUE"
        query_exists_constraint = "CREATE CONSTRAINT ON (texte:Texte_de_loi) ASSERT exists(texte.uid)"
        session.run(query=query_unique_constraint)
        session.run(query=query_exists_constraint)
    except :
        print('Already created constraint') 


def storeTextDeLoiInNeo(thisTexteDeLoi):
    printDev = False
    #print(thisTexteDeLoi)
    query = "merge (t:Texte_de_loi{ uid:'"+str(thisTexteDeLoi["uid"]) +"' , type: '"+str(thisTexteDeLoi["type"])+"', titrePrincipal: \""+str(thisTexteDeLoi["titrePrincipal"])+"\", dateCreation : '"+str(thisTexteDeLoi["dateCreation"])+"', datePublication : '"+str(thisTexteDeLoi["datePublication"])+"'});"   
    session.run(query=query)
    if printDev: print("\n\t -> Création du texte de loi ", thisTexteDeLoi['uid'], " OK")
    

def storeOrganeVoteInNeo(thisOrganesVotes,idTexteDeLoi,idVote):
    printDev = False
    if printDev: print("\n START STORING ORGANS VOTES FOR TEXTE DE LOI : ", idTexteDeLoi)
    for organeLink in thisOrganesVotes:
        query = "match(o:Organe{uid:'"+str(organeLink["organeRef"])+"'}),(t:Texte_de_loi{uid:'"+str(idTexteDeLoi)+ "'}) merge(o)-[v:VOTEDFOR{ voteId : '"+str(idVote) + "', nombreMembresGroupe: '"+str(organeLink["nombreMembresGroupe"])+"', positionMajoritaire: '"+ str(organeLink["positionMajoritaire"])+"', decompteVoix_nonVotants: '"+str(organeLink["decompteVoix_nonVotants"])+"',decompteVoix_pour: '"+str(organeLink["decompteVoix_pour"])+"',decompteVoix_contre:'"+ str(organeLink["decompteVoix_contre"])+"',decompteVoix_abstentions:'"+ str(organeLink["decompteVoix_abstentions"])+"', decompteVoix_nonVotantsVolontaires :'"+ organeLink["decompteVoix_nonVotantsVolontaires"]+ "' }] -> (t);"
        #query = "merge(t:Texte_de_loi{uid:'"+str(idTexteDeLoi)+"'}) ,(o:Organe{uid:'"+(str(organeLink["organeRef"]) )+"'}) merge (t)-[l:ISVOTEORGANE{nombreMembresGroupe: '"+str(organeLink["nombreMembresGroupe"])+"', positionMajoritaire: '"+ str(organeLink["positionMajoritaire"])+"', decompteVoix_nonVotants: '"+str(organeLink["decompteVoix_nonVotants"])+"',decompteVoix_pour: '"+str(organeLink["decompteVoix_pour"])+"',decompteVoix_contre:'"+ str(organeLink["decompteVoix_contre"])+"',decompteVoix_abstentions:'"+ str(organeLink["decompteVoix_abstentions"])+"', decompteVoix_nonVotantsVolontaires :'"+ organeLink["decompteVoix_nonVotantsVolontaires"]+ "'  }] -> (o) ;"
        session.run(query=query)
        if printDev: print("\t -> Création de la relation  ", idTexteDeLoi, "-> ", organeLink["organeRef"]," OK")

    if printDev: print("\n DONE STORING ORGANS VOTES FOR TEXTE DE LOI : ", idTexteDeLoi)

def storeActorVoteInNeo(thisActeurVotes, idTexteDeLoi,idVote):
    printDev = False
    if printDev: print("\n START STORING ACTORS VOTES FOR TEXTE DE LOI : ", idTexteDeLoi)

    for actorLink in thisActeurVotes:        
        #parDelegation = actorLink.get("parDelegation") if(actorLink.get("parDelegation")) else None       
        query = "match (a:Acteur{uid:'"+str(actorLink["acteurRef"])+ "'}),(t:Texte_de_loi{uid:'"+str(idTexteDeLoi)+"'}) merge (a)-[v:VOTEDFOR{ voteId : '"+str(idVote) + "', parDelegation: '"+ str(actorLink["parDelegation"]) +"', causePositionVote: '"+ str(actorLink["causePositionVote"])+"', positionVote: '"+str(actorLink["positionVote"])+"',organeRef: '"+str(actorLink["organeRef"])+ "'  }] -> (t) ;"
        session.run(query=query)
        if printDev: print("\t -> Création de la relation  ", idTexteDeLoi, "-> ", actorLink["acteurRef"]," OK") 

    if printDev: print("\n DONE STORING ORGANS VOTES FOR TEXTE DE LOI : ", idTexteDeLoi)

def storeAll(idTexteDeLoi,thisOrganesVotes, thisActeurVotes,idVote):
    printDev = False
    if printDev: print("\nDEBUT CREATION DES VOTES DU TEXTE DE LOI : "+str(idTexteDeLoi))  
    storeOrganeVoteInNeo(thisOrganesVotes, idTexteDeLoi,idVote)
    storeActorVoteInNeo(thisActeurVotes,idTexteDeLoi, idVote)
    if printDev: print("\nFIN CREATION D'UN TEXTE DE LOI ET SES VOTES")



def extractVoteInformation(id):
    for vote in scrutinsList:
        if str(vote.get("uid")) == str(id):
            groupeVotes = vote.get("groupVotes")
            acteurVotes = vote.get("actorVotes")
            #scrutinsList.remove(vote)  #on retire le vote de la liste une fois qu'on l'a extrait pour optimiser les prochaines itérations
            return groupeVotes, acteurVotes

            
def getAllTextDeLoi(path):
    printDev = True
    votesCounter = 0
    voteActeurCounter = 0
    voteOrganCounter = 0
    storedTexteList = set()  #monitor the unique texte de lois
    
    with open(path) as file:
        data = json.load(file)
        for item in data:
            if item.get("texteDeLoi"): #on verifie qu'il y a bien un projet de loi associé
                #if printDev: print("\nExtracting the  Texte de loi for the vote with id : "+ item.get("vote").get("uid"))
                votesCounter +=1
                if votesCounter>1550:
                    for texte in item.get("texteDeLoi"): #on boucle sur tous les texte de lois disponibles
                        
                        #if texte.get("uid") == "PRJLANR5L15B0118":
                        if "PRJLAN" in texte.get("uid"): #on prends uniquement les text de lois de l'assemblé nationale
                            #if printDev: print("\nExtracting information for the the text de loi with id : "+texte.get("uid"))                        
                            
                            if (texte.get("uid") not in storedTexteList): #on verifie si le projet de loi n'est oas deja crée
                                thisTexteDeLoi = {
                                    "uid" : texte.get("uid"),
                                    "type" : texte.get("type"),
                                    "titrePrincipal" : texte.get("titrePrincipal"),
                                    "dateCreation" : texte.get("dateCreation").split('T')[0] if(texte.get("dateCreation")) else None,#on prends uniquement la date sans l'heure
                                    "datePublication" : texte.get("datePublication").split('T')[0] if(texte.get("datePublication")) else None,
                                    "voteRef" : item.get("vote").get("uid") #on garde l'id du vote pour pouvoir faire le lien entre le txt de loi et le vote 
                                }
                                storeTextDeLoiInNeo(thisTexteDeLoi) #on stock le texte 
                                storedTexteList.add(texte.get("uid")) #on l'ajoute a la list des text  créer
    
                            groupeVotes, acteurVotes = extractVoteInformation(item.get("vote").get("uid"))
                            
                            #Pour chaque vote on extrait les organes et les acteurs qui y ont pris part                   
                            storeAll(texte.get("uid"),groupeVotes,acteurVotes,item.get("vote").get("uid")) 
                            #voteActeurCounter+=len(acteurVotes)
                            #voteOrganCounter +=len(groupeVotes)    
                    if votesCounter%50==0 : print(str(votesCounter)+"/3958")            
                    #if printDev : print("\n Latest vote stored : "+ str(item.get("vote").get("uid")) +"\t Number of votes stored : "+str(votesCounter)+" /3958")                    
                    # stop à 1550
                    
            #if(votesCounter >=10): #limit to stop iterations in case of sampeling
                #break                          
    #print("\nALL DONE STORED\n ",votesCounter, "TOTAL VOTES  FROM ",len(storedTexteList) , "TEXTES DE LOI\n ->"+str(voteActeurCounter)+" TOTAL ACTOR VOTES LINKS \n ->"+str(voteOrganCounter)+" TOTAL ORGANES VOTES LINKS ")

def lancheringestion():   
    print("\n\n=== PROJET NEO4J - IMPORT  TEXTE DE LOI, ACTORS-VOTES RELATIONSHIPS AND ORGANES-VOTE RELATIONSHIPS IN NEO4J===\n")  
    with open(votesFile) as file:
        scrutinsList = json.load(file)
    #createContraints()      
    getAllTextDeLoi(textDeloiFile)
    session.close()   
    print("\n\n=== PROJET NEO4J - IMPORT  TEXTE DE LOI, ACTORS-VOTES RELATIONSHIPS AND ORGANES-VOTE RELATIONSHIPS IN NEO4J===\n")