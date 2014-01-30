#put_data_into_mysql.py
import json
import pandas
import csv
import parameters

def create_states_dataframe(input_data):
    # Create DataFrame for States
    data = []
    for row in input_data:                    
        d = json.loads(row)
        story_id = d['data']['story_id']        
        # Create State Vector
        statedata = {}
        for e in d['data']['citiesbycountry']:
            state = e[0][3:5]
            if state in statedata:
                statedata[state] += e[1]
            else:
                statedata[state] = e[1]
        statevector = []
        for state in parameters.state_names():
            if state in statedata:
                statevector.append(statedata[state])
            else:
                statevector.append(0)
        new_row = [story_id] + statevector
        data.append(new_row)
    states_dataframe = pandas.DataFrame(data, columns = ['story_id'] + parameters.state_names())     
    return states_dataframe
    
def create_countries_dataframe(input_data): 
    data = []
    for row in input_data:                    
        d = json.loads(row)
        story_id = d['data']['story_id']
                        
        # Create Country Vector
        us_count = 0
        count = 0
        for e in d['data']['countries']:
            if e[0] == 'us':
                us_count = e[1]
            if e[0] == "_COUNT_":
                count = e[1]       
        new_row = [story_id, count, us_count]
        data.append(new_row)
    countries_dataframe = pandas.DataFrame(data, columns = ['story_id', 'count', 'us'])
    return countries_dataframe
    
def create_cities_dataframe(input_data): 
    # Find city names
    data = []
    for row in input_data:                    
        d = json.loads(row)
        data.append(d['data']['citiesbycountry'])
        
    cities = {}
    for row in data:
        for unit in row:
            city = unit[0]
            if city not in cities:
                cities[city] = 1

    # Create DataFrame for Cities
    column_names = sorted(cities.keys())
    with open('data\cities.json') as jsonfile: 
        data = []
        for row in jsonfile:                    
            d = json.loads(row)
            story_id = d['data']['story_id']
            # Create City Vector
            citydata = {}
            for e in d['data']['citiesbycountry']:
                citydata[e[0]] = e[1]
            cityvector = []    
            for city in column_names:
                if city in citydata:
                    cityvector.append(citydata[city])
                else:
                    cityvector.append(0)
            new_row = [story_id] + cityvector
            data.append(new_row)        
    cities_dataframe = pandas.DataFrame(data, columns = ['story_id']+column_names) 
    return cities_dataframe
    
def create_linkinfo_dataframe(input_data, story_id_list):
    bad_rows = []
    data = []
    i = 0
    for row in input_data:
        try:
            d = json.loads(row)
        except:
            new_row = ["", 0, "", "", "", "", 0] #placeholder row
            data.append(new_row)
            bad_rows.append(i)
            i += 1
            continue
        story_id = story_id_list[i]
        rate = d['data']['rate']
        thumbnail_url = d['data']['thumbnail_url']
        description = d['data']['description'].encode('utf-8')
        title = d['data']['title'].encode('utf-8')
        url = d['data']['url']
        clicks = d['data']['clicks']
        new_row = [story_id, rate, thumbnail_url, description, title, url, clicks]    
        data.append(new_row)
        i += 1
    linkinfo_dataframe = pandas.DataFrame(data, columns = ['story_id', 'rate', 'thumbnail_url', 'description', 'title', 'url', 'clicks'])        
    return linkinfo_dataframe, bad_rows   
    
    
def create_ghashes_csv(filename, global_hashes, storyids):
    with open(filename, 'wb') as file:
        writer = csv.writer(file)
        for i in range(0, len(global_hashes)):
            writer.writerow([global_hashes[i], storyids[i]])
       
