#master_pipeline.py

import MySQLdb as MS
import pandas
import operator

import parameters
import scrape_bitly
import clean_data
import sql_wrapper
import analyze
reload(scrape_bitly)
reload(clean_data)
reload(sql_wrapper)
reload(analyze)

topic_for_search = "%22state%20of%20the%20union%22"
topic_name = "state_of_the_union"

#### Scrape bit.ly and store data in lists of json lines
print 'scraping...'
html = scrape_bitly.search_topic(topic_for_search, num_rows = 60)
global_hashes = scrape_bitly.parse_search_results(html)
story_ids = scrape_bitly.get_storyIDs(global_hashes)

print 'scraping...'
cities_data = scrape_bitly.get_cities_data(story_ids)
print 'scraping...'
countries_data = scrape_bitly.get_countries_data(story_ids)
print 'scraping...'
linkinfo_data = scrape_bitly.get_linkinfo_data(global_hashes)
print 'calculating...'

####--- Store data in dataframes -------#    
states_dataframe = clean_data.create_states_dataframe(cities_data)
countries_dataframe = clean_data.create_countries_dataframe(countries_data)
linkinfo_dataframe, bad_rows = clean_data.create_linkinfo_dataframe(linkinfo_data, countries_dataframe['story_id'])
print bad_rows
states_dataframe = states_dataframe.drop(bad_rows)
countries_dataframe = countries_dataframe.drop(bad_rows)
linkinfo_dataframe = linkinfo_dataframe.drop(bad_rows)

ghashes_dataframe = pandas.DataFrame(global_hashes)
ghashes_dataframe['1'] = story_ids

# This part for safe-keeping
cities_dataframe = clean_data.create_cities_dataframe(cities_data)
cities_dataframe = cities_dataframe.drop(bad_rows)
cities_tablename = topic_name + "_cities"
cities_dataframe.to_csv('C:\\Program Files\\MySQL\\MySQL Server 5.6\\data\\' + cities_tablename + '.csv')

###----  Put dataframes into MySQL --------#
## select database
db = MS.connect(host="localhost", user = "root")
with db:
    cursor = db.cursor()
    sql = 'USE bubble'
    cursor.execute(sql)
    
states_tablename = topic_name + "_states"
sql_wrapper.create_states_table(db, states_tablename) 
sql_wrapper.load_table_data(db, states_tablename, states_dataframe)

countries_tablename = topic_name + "_countries"
sql_wrapper.create_countries_table(db, countries_tablename)
sql_wrapper.load_table_data(db, countries_tablename, countries_dataframe)

ghashes_tablename = topic_name + "_ghashes"
sql_wrapper.create_ghashes_table(db, ghashes_tablename)
sql_wrapper.load_table_data(db, ghashes_tablename, ghashes_dataframe)

linkinfo_tablename = topic_name +"_linkinfo"
filename = 'C:\\Program Files\\MySQL\\MySQL Server 5.6\\data\\' + linkinfo_tablename + '.csv'
linkinfo_dataframe.to_csv(filename, header = False, sep = '>')

sql_wrapper.create_linkinfo_table(db, linkinfo_tablename)
sql_filename = 'C:/Program Files/MySQL/MySQL Server 5.6/data/' + linkinfo_tablename + '.csv'
sql_query = "LOAD DATA LOCAL INFILE '" + sql_filename + "' INTO TABLE " + linkinfo_tablename + " FIELDS TERMINATED BY '>' LINES TERMINATED BY  '\r\n'"
    
with db:
    cursor = db.cursor()
    cursor.execute(sql_query)

### Load from MySQL ###########################################################

#linkinfo_tablename = 'abortion_linkinfo'
#states_tablename = 'abortion_states'
#countries_tablename = 'abortion_countries'
#topic_name = 'abortion'

def tuple_to_list(rows):
    data = []
    for row in rows:
        new_row = []
        for e in row:
            new_row.append(e)
        data.append(new_row)
    return data



# Retrieve linkinfo 
sql_query = "SELECT * FROM " + linkinfo_tablename
cursor.execute(sql_query)
rows = cursor.fetchall()
data = tuple_to_list(rows)
linkinfo = pandas.DataFrame(data, columns = ['old_ix', 'story_id', 'rate', 'thumbnail_url', 'description', 'title', 'url', 'clicks'])
  
# Retrieve state data 
sql_query = "SELECT * FROM " + states_tablename
cursor.execute(sql_query)
rows = cursor.fetchall()
data = tuple_to_list(rows)
states = pandas.DataFrame(data, columns = ['old_ix', 'story_id'] + parameters.state_names())

# Retrieve country data
sql_query = "SELECT * FROM " + countries_tablename
cursor.execute(sql_query)
rows = cursor.fetchall()
data = tuple_to_list(rows)
countries = pandas.DataFrame(data, columns = ['old_ix', 'story_id', 'count', 'us'])


info = {}
for ix, row in linkinfo.iterrows():
    story_id = row['story_id']
    info[story_id] ={}
    info[story_id]['title'] = row['title']
    info[story_id]['description'] = row['description']
    info[story_id]['url'] = row['url']
    info[story_id]['clicks'] = row['clicks']
    
for ix, row in countries.iterrows():
    story_id = row['story_id']
    US = row['us']
    info[story_id]['us'] = US    

###### Analyze  ###################################################################

# without clicks
itemPrefs = analyze.create_matrix(states) 
statePrefs = analyze.transformPrefs(itemPrefs)

# include number of clicks
itemPrefsClicks = {}
for ID in itemPrefs:
    clicks = info[ID]['clicks']
    itemPrefsClicks[ID] = {}
    for state in itemPrefs[ID]:
        itemPrefsClicks[ID][state] = itemPrefs[ID][state] * clicks
statePrefsClicks = analyze.transformPrefs(itemPrefsClicks)        
        
# create favorites dataframe - includes clicks
statePrefsClicks_sorted = sort_favorites(statePrefsClicks)
data = [] 
for S in parameters.state_names():
    for e in statePrefsClicks_sorted[S]:
        ID = e[0]
        new_row = [S, ID, int(round(info[ID]['clicks']*info[ID]['us'] *  itemPrefs[ID]['ca']))]
        data.append(new_row)
favorites_dataframe = pandas.DataFrame(data)

# find seed for each state - without clicks    
seed = {}
for eachState in statePrefs:
    seed[eachState] = max(statePrefs[eachState], key = lambda x: statePrefs[eachState].get(x) )  
 
# calculate results 
results = {}    
for eachState in favorite:
    results[eachState] = analyze.topDisimilarItems(itemPrefs, seed[eachState], n = 10, similarity = analyze.sim_distance)
 
# sort results by clicks
results_sorted = {} 
for each in results:
    results_sorted[each] =  sorted(results[each], key = lambda each: info[each[1]]['clicks'], reverse = True)

# put results into dataframe
data = [] 
for S in parameters.state_names():
    for e in results_sorted[S]:
        ID = e[1]
        new_row = [S, ID, int(round(info[ID]['clicks']*info[ID]['us'] *  itemPrefs[ID]['ca'])), topStateforItem(itemPrefs, ID), int(round(info[ID]['us']*info[ID]['clicks']*itemPrefs[ID][topStateforItem(itemPrefs, ID)]))]
        data.append(new_row)
dissim_dataframe = pandas.DataFrame(data) 

# put results in database

tablename = topic_name + '_favorites_algo2'
sql_wrapper.create_favorites_table_algo2(db, tablename)
sql_wrapper.load_table_data(db, tablename, favorites_dataframe)

tablename =  topic_name + '_results_algo2'
sql_wrapper.create_results_table_algo2(db, tablename)
sql_wrapper.load_table_data(db, tablename, dissim_dataframe)
