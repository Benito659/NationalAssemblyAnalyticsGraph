from neo4j import GraphDatabase
import json
import os

# data_base_connection = GraphDatabase.driver(uri = "bolt://neo4j:7687", auth=("neo4j", "123narutoC."))
import os
db_host = os.environ.get('NEO4J_HOST', 'localhost')
db_port = os.environ.get('NEO4J_PORT', '7687')
print("db_host == ",db_host,"db_port == ",db_port)
data_base_connection = GraphDatabase.driver(uri='bolt://project-neo4jplus-1:7687',auth=("neo4j", "123narutoC."))


session = data_base_connection.session()   
repertoire ="./data/organe"
selectedOrganeType = [
    "API","ASSEMBLEE","CMP","CNPE","CNPS","COMNL","COMPER",
    "COMSENAT","COMSPSENAT","CONFPT","CONSTITU","GA",
    "GOUVERNEMENT","GP","GROUPESENAT","MINISTERE","ORGAINT",
    "ORGEXTPARL","PARPOL","PRESREP","SENAT"
]
organeList = []
def cleanLibelle(libelle):
    return libelle.replace('"','\"').replace("'",'\"')


def storeInNeo():
    for organe in organeList:
        query = "Create (o:organe{uid:'"+ str(organe[0]) +"' , codeType:'"+ str(organe[1]) +"' , libelle:'"+ str(cleanLibelle(organe[2])) +"'})"
        session.run(query=query)      

def readJsonFile(fileName):
    with open(fileName) as file:
        data = json.load(file)
        if data['organe'].get("codeType") in selectedOrganeType:
            organeTampon = []
            temp = data['organe'].get("uid")
            organeTampon.append(temp)
            temp = data['organe'].get("codeType")
            organeTampon.append(temp)
            temp = data['organe'].get("libelle")
            organeTampon.append(temp)
            temp = data['organe'].get("viMoDe")
            organeTampon.append(temp)   
            organeList.append(organeTampon)
            
    
        

def getAllJsonFiles(basepath) : 
    for entry in os.listdir(basepath):
        if os.path.isfile(os.path.join(basepath, entry)):
            extension = os.path.splitext(entry)[1]
            if extension == ".json":                
                readJsonFile(os.path.join(basepath, entry))

    

def lancheringestion():
    print("\n=== PROJET NEO4J - IMPORT ORGANES IN NEO4J===\n")  
    getAllJsonFiles(repertoire)  
    print(organeList)    
    storeInNeo()
    session.close()
    print("\nAL UNIQUE ORGANES IMPORTED SUCCESSFULLY ")    
    print("\n=== PROJET NEO4J - IMPORT ORGANES IN NEO4J ===\n")
    





















