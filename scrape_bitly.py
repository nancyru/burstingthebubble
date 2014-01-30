#scrape_bitly.py

import urllib2
import time
import json
import random

def url_request_delay():
    return random.uniform(1, 2)

def request_url(url):
    """Gets a webpage's HTML."""
    req = urllib2.Request(url)
    req.add_header('Cookie', '_user=bnJ1NDUx|1390004714|acf0bf5399ecd3143e8d3ea820b057d16889c66e; user=bnJ1NDUx|1390004715|8f739de808c342cb4297c4cb84395ff55ebdc9e3; __utma=161486207.1109950805.1390004719.1390244082.1390248054.4; __utmb=161486207.1.10.1390248054; __utmc=161486207; __utmz=161486207.1390004719.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)')
    handle = urllib2.urlopen(req)
    html = handle.read()
    return html
    
def parse_search_results(html):    
    j = json.loads(html)
    global_hashes = []
    for h in j['data']['results']:
        global_hashes.append(h['globalhash'])
    return global_hashes    
                    
def search_topic(topic, num_rows):
    url = 'http://rt.ly/search?rows=' + str(num_rows) + '&start=0&q=' + topic + '&lang=en'
    html = request_url(url)    
    return html

def get_storyIDs(global_hashes):    
    story_ids = []
    for g in global_hashes:
        url = 'http://rt.ly/_/storyapi/story_id/get?ghashes=' + g
        html = request_url(url)
        j = json.loads(html)
        story_id = j['data']['story_id']
        story_ids.append(story_id)
        time.sleep(url_request_delay())
    return story_ids  

def get_cities_data(story_ids):
    cities_data = []
    for s in story_ids:
        url = 'http://rt.ly/_/storyapi/distribution?field=citiesbycountry&country=us&story_id=' + s
        html = request_url(url)
        cities_data.append(html)
        time.sleep(url_request_delay())
    return cities_data  

def get_countries_data(story_ids):
    countries_data = []
    for s in story_ids:
        url = 'http://rt.ly/_/storyapi/distribution?field=countries&story_id=' + s
        html = request_url(url)
        countries_data.append(html)
        time.sleep(url_request_delay())
    return countries_data    
    
def get_linkinfo_data(global_hashes):      
    linkinfo_data = []
    for g in global_hashes:
        url = 'http://rt.ly/linkinfo?ghash=' + g 
        try:
            html = request_url(url)
        except:
            html = '{"status_code": 200, "data": {"rate": 0, "thumbnail_url": "", "description": "", "title": "", "url": "", "clicks": }, "status_txt": "OK"}'
        linkinfo_data.append(html)        
        time.sleep(url_request_delay())
    return linkinfo_data

def output_json(data, filename):
    with open(filename, 'wb') as file:
        for e in data:
            file.write(e)
            file.write('\n')
            








