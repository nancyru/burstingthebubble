#sql_wrapper.py

import MySQLdb as MS 
   
def create_states_table(db, table_name):
    sql_query = "CREATE TABLE " +  table_name + "(ix int NOT NULL, story_id char(32) NOT NULL, "    
    for i in range(0, 53):
        sql_query = sql_query + 'state'+str(i)+'     ' + "float" + "     " + "NOT NULL,"
    sql_query = sql_query + "PRIMARY KEY (ix) )ENGINE = InnoDB"  
    
    with db:
        cursor = db.cursor()
        cursor.execute(sql_query) 

def create_countries_table(db, table_name):
    sql_query = "CREATE TABLE " +  table_name + "(ix int NOT NULL, story_id char(32) NOT NULL, count int NOT NULL, us float NOT NULL,"    
    sql_query = sql_query + "PRIMARY KEY (ix) )ENGINE = InnoDB" 
    
    with db:
        cursor = db.cursor()
        cursor.execute(sql_query)  

def create_ghashes_table(db, table_name):
    sql_query =  "CREATE TABLE " + table_name + " ( "
    sql_query += "ix			int			NOT NULL,"	
    sql_query += "ghash		char(7) 	NOT NULL,"
    sql_query += "story_id	char(32) 	NOT NULL,"
    sql_query += "PRIMARY KEY (ix)"
    sql_query += ") ENGINE = InnoDB"

    with db:
        cursor = db.cursor()
        cursor.execute(sql_query)

def create_linkinfo_table(db, table_name):
    sql_query =  "CREATE TABLE " + table_name + " ( "
    sql_query += "ix			int			NOT NULL, "
    sql_query += "story_id	    char(32)    NOT NULL, "
    sql_query += "rate		    float		NOT NULL, "
    sql_query += "thumbnail_url	text	    NOT NULL, "
    sql_query += "description	text	    NOT NULL, "
    sql_query += "title			text	    NOT NULL, "
    sql_query += "url			text		NOT NULL, "
    sql_query += "clicks		int			NOT NULL, "
    sql_query += "PRIMARY KEY (ix)"
    sql_query += ") ENGINE = InnoDB"

    with db:
        cursor = db.cursor()
        cursor.execute(sql_query)
        
def load_table_data(db, table_name, df):
    for ix, row in df.iterrows():
        row_data = "'" + str(ix) + "', "
        for key in df.columns:
            row_data += "'" + str(row[key]) + "', "
        row_data = row_data[:-2]    
        
        sql_query = "INSERT INTO " + table_name
        sql_query += " VALUES(" + row_data + ")"
     
        with db:
            cursor = db.cursor()
            cursor.execute(sql_query) 

            
def load_table_linkinfo(db, table_name, df):
    # this does not work
    for ix, row in df.iterrows():
        row_data = '"' + str(ix) + "', "
        row_data += '"' + str(row['story_id']) + '", '
        row_data += '"' + str(row['rate']) + '", '
        row_data += '"' + str(row['thumbnail_url']) + '", '
        row_data += '"' + str(row['description']).replace('"', "'") + '", '
        row_data += '"' + str(row['title']).replace('"', "'") + '", '
        row_data += '"' + str(row['url']) + '", '
        row_data += '"' + str(row['clicks']) + '"'
        print row_data
        sql_query = "INSERT INTO " + table_name
        sql_query += " VALUES(" + row_data + ")"
        print sql_query
  
        with db:
            cursor = db.cursor()
            cursor.execute(sql_query) 
            
def retrieve_linkinfo(cursor, tablename):
    # Retrieve linkinfo  
    sql_query = "SELECT * FROM " + tablename
    print sql_query     
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    data = tuple_to_list(rows)
    linkinfo = pandas.DataFrame(data, columns = ['old_ix', 'story_id', 'rate', 'thumbnail_url', 'description', 'title', 'url', 'clicks'])
    return linkinfo

def create_results_table(db, tablename):
    sql_query = "CREATE TABLE " +  tablename + "(ix int NOT NULL, state char(2) NOT NULL, story_id char(32) NOT NULL, "    
    sql_query = sql_query + "PRIMARY KEY (ix) )ENGINE = InnoDB"  
    
    with db:
        cursor = db.cursor()
        cursor.execute(sql_query) 
        
def create_favorites_table_algo2(db, tablename):
    sql_query = "CREATE TABLE " +  tablename + "(ix int NOT NULL, state char(2) NOT NULL, story_id char(32) NOT NULL, state_clicks int NOT NULL, "  
    sql_query = sql_query + "PRIMARY KEY (ix) )ENGINE = InnoDB"  
    
    with db:
        cursor = db.cursor()
        cursor.execute(sql_query)  

def create_results_table_algo2(db, tablename):
    sql_query = "CREATE TABLE " +  tablename + "(ix int NOT NULL, state char(2) NOT NULL, story_id char(32) NOT NULL, state_clicks int NOT NULL, top_state char(2) NOT NULL, top_state_clicks int NOT NULL,"  
    sql_query = sql_query + "PRIMARY KEY (ix) )ENGINE = InnoDB"  
    
    with db:
        cursor = db.cursor()
        cursor.execute(sql_query)        

def retrieve_results(cursor, topic_name, state):
    sql_query = "SELECT * FROM " + topic_name + "_linkinfo" 
    sql_query += " INNER JOIN " + topic_name + "_results_dissim"
    sql_query += " ON " + topic_name + "_results_dissim.story_id = " + topic_name + "_linkinfo.story_id"
    sql_query += " WHERE state = '" + state + "'"
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    data = tuple_to_list(rows)
    return data
    
