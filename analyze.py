#analyze.py

# similarity by city
# find article with highest percentage in SF --  ix 6

# find article with highest percentage in CA -- ix 37 huffingtonpost 80% of 89 clicks (worldwide)
# 99063c98ac48a16aebf1217c9adebcec

# item-based collaborative filtering

from math import sqrt
import parameters

def create_matrix(states):
    # Create item-centric preference matrix from DataFrame
    itemPrefs = {}
    for idx, row in states.iterrows():
        itemPrefs[row["story_id"]] = {}
        for st in parameters.state_names():
            itemPrefs[row["story_id"]][st] = row[st]
    return itemPrefs
    
def create_matrix_cities(cities):
   # Create item-centric preference matrix from cities DataFrame
    itemPrefs = {}
    for idx, row in cities.iterrows():
        itemPrefs[row["story_id"]] = {}
        for st in cities.columns[2:]:
            itemPrefs[row["story_id"]][st] = row[st]
    return itemPrefs  
    
def transformPrefs(prefs):
  result={}
  for person in prefs:
    for item in prefs[person]:
      result.setdefault(item,{})
      
      # Flip item and person
      result[item][person]=prefs[person][item]
  return result    

def sim_distance(prefs,person1,person2):
    # Get the list of shared_items
    si={}
    for item in prefs[person1]: 
        if item in prefs[person2]: si[item]=1

    # if they have no ratings in common, return 0
    if len(si)==0: return 0

    # Add up the squares of all the differences
    sum_of_squares=sum([pow(prefs[person1][item]-prefs[person2][item],2) 
                      for item in si])

    return 1/(1+sum_of_squares)   
            
def sim_pearson(prefs,p1,p2):
    # Get the list of mutually rated items
    si={}
    for item in prefs[p1]: 
        if item in prefs[p2]: si[item]=1

    # if they are no ratings in common, return 0
    if len(si)==0: return 0

    # Sum calculations
    n=len(si)
  
    # Sums of all the preferences
    sum1=sum([prefs[p1][it] for it in si])
    sum2=sum([prefs[p2][it] for it in si])
  
    # Sums of the squares
    sum1Sq=sum([pow(prefs[p1][it],2) for it in si])
    sum2Sq=sum([pow(prefs[p2][it],2) for it in si])	
  
    # Sum of the products
    pSum=sum([prefs[p1][it]*prefs[p2][it] for it in si])
  
    # Calculate r (Pearson score)
    num=pSum-(sum1*sum2/n)
    den=sqrt((sum1Sq-pow(sum1,2)/n)*(sum2Sq-pow(sum2,2)/n))
    if den==0: return 0

    r=num/den

    return r    

    
def topMatches(prefs,item,n=5,similarity=sim_pearson):
    scores=[(similarity(prefs,item,other),other) 
                  for other in prefs if other!=item]
    scores.sort()
    scores.reverse()
    return scores[0:n] 

def topDisimilarItems(prefs,item,n=5,similarity=sim_pearson):
    scores=[(similarity(prefs,item,other),other) 
                  for other in prefs if other!=item]
    scores.sort()
    return scores[0:n]

def calculateSimilarItems(prefs,n=10):
    # Create a dictionary of items showing which other items they
    # are most similar to.
    result={}
    for item in itemPrefs:
        # Status updates for large datasets
        # Find the most similar items to this one
        scores=topMatches(itemPrefs,item,n=n,similarity=sim_distance)
        result[item]=scores
    return result 

def calculateDisimilarItems(prefs,n=10):
    # Create a dictionary of items showing which other items they
    # are most similar to.
    result={}
    for item in prefs:
        # Status updates for large datasets
        # Find the most similar items to this one
        scores=topDisimilarItems(prefs,item,n=n,similarity=sim_distance)
        result[item]=scores
    return result   
  
def displayResults(result, story_id):
    with open('output.txt', 'wb') as file:
        for item in result[story_id]:
            file.write('<p>')
            file.write('<b>')
            file.write('%s' %info[item[1]]['title'])
            file.write('</b>')
            file.write('<BR>\n')
            file.write('%s' %info[item[1]]['description'])
            file.write('<BR>\n')
            file.write('<A HREF = "')
            file.write('%s' %info[item[1]]['url'])
            file.write('">')
            file.write('%s' %info[item[1]]['url'])
            file.write('</A>')
            file.write('</p>\n')
            
            
