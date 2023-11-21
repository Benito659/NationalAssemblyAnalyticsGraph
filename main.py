from flask import Flask, render_template, request, jsonify, make_response
import allQueries as query
import allStoredQueries as storedQuery
import importSample
import importOrganes
import addDeuputePictures
import importTextDeLoi
import importJSON
import fastImport

app = Flask(__name__, static_url_path='', static_folder='web/static', template_folder='web/templates')


max = 4 #Number of results to return for the queries
data = {}  
storedData = {}
if(query.testDataSet()):#on verifie si notre data set est chargé sur la machine hôte
    data["youngest"]= query.getYoungestActors(max) #députés les plus jeunes
    data["changers"] = query.getMostChangePartyPol(max) #députés qui ont le plus changés de groupe politique
    data["ministersParties"] = query.getPartiesWithMostMinister(max) #parties politiques avec le plus de ministres
    data["delegators"] = query.getMostVoteDelegator(max)  #les députés qui delèguent le plus leur votes
    data["abstentions"] = query.getTexteWithMostAbstentions(max) #les projets de lois avec le plus d'indécisions
    data["contestations"] = query.getTexteWithMostVotesContres(max)  #les projets de loi avec les plus de votes contre
    data["earlyDeaths"] = query.getEarliestDead(max) #députés décédes le plus vite après leur nomination a une legislature
    data["diffVoters"] = query.getMostVoteDifferentFromParpol(max)  #députés qui votent le plus contre leur partie
    data["diffUniqueVoters"] = query.getMostVoteUniqueDifferentFromParpol(max) #personnes qui ont été les seuls à voter diff de tous les autres membres de leurs parties
    data["opposants"] = storedQuery.getPartiesWithMostOppositionToTexte(max)  #Quels groupes politiques ont le + voté contre les projets de loi du gouvernement
    data["externalSupport"] = storedQuery.getAmandementWithMostExternalSupport(max) # le nombre d'amendements soutenu par des cosignataires qui ne sont pas dans le même parti que le signataire
    data["mostAmandement"] = storedQuery.getArticleWithMostAmandements(max) #Quels articles de lois on subits le plus d'amendements ?
    
    #loading stored queries results
storedData["youngest"]= query.getYoungestActors(max) #députés les plus jeunes
storedData["changers"] = query.getMostChangePartyPol(max) #députés qui ont le plus changés de groupe politique
storedData["ministersParties"] = query.getPartiesWithMostMinister(max) #parties politiques avec le plus de ministres
storedData["delegators"] = query.getMostVoteDelegator(max)  #les députés qui delèguent le plus leur votes
storedData["abstentions"] = query.getTexteWithMostAbstentions(max) #les projets de lois avec le plus d'indécisions
storedData["contestations"] = query.getTexteWithMostVotesContres(max)  #les projets de loi avec les plus de votes contre
storedData["earlyDeaths"] = query.getEarliestDead(max) #députés décédes le plus vite après leur nomination a une legislature
storedData["diffVoters"] = query.getMostVoteDifferentFromParpol(max)  #députés qui votent le plus contre leur partie
storedData["diffUniqueVoters"] = query.getMostVoteUniqueDifferentFromParpol(max) #personnes qui ont été les seuls à voter diff de tous les autres membres de leurs parties
storedData["opposants"] = storedQuery.getPartiesWithMostOppositionToTexte(max)  #Quels groupes politiques ont le + voté contre les projets de loi du gouvernement
storedData["externalSupport"] = storedQuery.getAmandementWithMostExternalSupport(max) # le nombre d'amendements soutenu par des cosignataires qui ne sont pas dans le même parti que le signataire
storedData["mostAmandement"] = storedQuery.getArticleWithMostAmandements(max) #Quels articles de lois on subits le plus d'amendements ?



@app.route("/")
def indexStored():
    """ Route to render the HTML """
    return render_template("index.html",data=storedData,limit=max)


@app.route("/dashboard")
def indexLive():
    """ Route to render the HTML """
    return render_template("dashboard.html",data=data,limit=max)

@app.route("/ingestion")
def ingestion():
    """ Route to render the HTML """
    importSample.lancheringestion()
    importOrganes.lancheringestion()
    addDeuputePictures.lancheringestion()
    importTextDeLoi.lancheringestion()
    importJSON.lancheringestion()
    fastImport.lancheringestion()
    return render_template("index.html",data=data,limit=max)



@app.route('/filteredDashboard', methods=['GET', 'POST'])
def filterLive():
    if request.method == 'POST':
        max = int(request.form.get('filter'))
        data = {}
        if(query.testDataSet()):
            data["youngest"]= query.getYoungestActors(max) #députés les plus jeunes
            data["changers"] = query.getMostChangePartyPol(max) #députés qui ont le plus changés de groupe politique
            data["ministersParties"] = query.getPartiesWithMostMinister(max) #parties politiques avec le plus de ministres
            data["delegators"] = query.getMostVoteDelegator(max)  #les députés qui delèguent le plus leur votes
            data["abstentions"] = query.getTexteWithMostAbstentions(max) #les projets de lois avec le plus d'indécisions
            data["contestations"] = query.getTexteWithMostVotesContres(max)  #les projets de loi avec les plus de votes contre
            data["earlyDeaths"] = query.getEarliestDead(max) #députés décédes le plus vite après leur nomination a une legislature
            data["diffVoters"] = query.getMostVoteDifferentFromParpol(max)  #députés qui votent le plus contre leur partie
            data["diffUniqueVoters"] = query.getMostVoteUniqueDifferentFromParpol(max) #personnes qui ont été les seuls à voter diff de tous les autres membres de leurs parties
            storedData["opposants"] = storedQuery.getPartiesWithMostOppositionToTexte(max)  #Quels groupes politiques ont le + voté contre les projets de loi du gouvernement
            storedData["externalSupport"] = storedQuery.getAmandementWithMostExternalSupport(max) # le nombre d'amendements soutenu par des cosignataires qui ne sont pas dans le même parti que le signataire
            storedData["mostAmandement"] = storedQuery.getArticleWithMostAmandements(max) #Quels articles de lois on subits le plus d'amendements ?
    return render_template("dashboard.html",data=data,limit=max)


@app.route('/filter', methods=['GET', 'POST'])
def filterStored():
    if request.method == 'POST':
        max = int(request.form.get('filter'))         
        storedData = {}        
        storedData["youngest"]= query.getYoungestActors(max) #députés les plus jeunes
        storedData["changers"] = query.getMostChangePartyPol(max) #députés qui ont le plus changés de groupe politique
        storedData["ministersParties"] = query.getPartiesWithMostMinister(max) #parties politiques avec le plus de ministres
        storedData["delegators"] = query.getMostVoteDelegator(max)  #les députés qui delèguent le plus leur votes
        storedData["abstentions"] = query.getTexteWithMostAbstentions(max) #les projets de lois avec le plus d'indécisions
        storedData["contestations"] = query.getTexteWithMostVotesContres(max)  #les projets de loi avec les plus de votes contre
        storedData["earlyDeaths"] = query.getEarliestDead(max) #députés décédes le plus vite après leur nomination a une legislature
        storedData["diffVoters"] = query.getMostVoteDifferentFromParpol(max)  #députés qui votent le plus contre leur partie
        storedData["diffUniqueVoters"] = query.getMostVoteUniqueDifferentFromParpol(max) #personnes qui ont été les seuls à voter diff de tous les autres membres de leurs parties
        storedData["opposants"] = storedQuery.getPartiesWithMostOppositionToTexte(max)  #Quels groupes politiques ont le + voté contre les projets de loi du gouvernement
        storedData["externalSupport"] = storedQuery.getAmandementWithMostExternalSupport(max) # le nombre d'amendements soutenu par des cosignataires qui ne sont pas dans le même parti que le signataire
        storedData["mostAmandement"] = storedQuery.getArticleWithMostAmandements(max) #Quels articles de lois on subits le plus d'amendements ?             
    return render_template("index.html",data=storedData,limit=max)


if __name__ == "__main__":
    app.run(debug=True,host='0.0.0.0')   







