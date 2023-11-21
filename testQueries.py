import allStoredQueries as query


#resultList = query.getYoungestActors(3)
#resultList = query.getMostChangePartyPol(3)
#resultList = query.getPartiesWithMostMinister(3)
#resultList = query.getMostVoteDelegator(3)
#resultList = query.getTexteWithMostAbstentions(10)
#resultList = query.getTexteWithMostVotesContres(10)
#resultList = query.getEarliestDead(3)
#resultList = query.getMostVoteDifferentFromParpol(3)
#resultList = query.getMostVoteUniqueDifferentFromParpol(3)
#resultList = query.getPartiesWithMostOppositionToTexte(3)
resultList = query.getAmandementWithMostExternalSupport(3)

for item in resultList:
    print(item)

#print(query.testDataSet())