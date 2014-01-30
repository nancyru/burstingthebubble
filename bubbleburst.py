from flask import Flask, render_template, request
import MySQLdb as MS
import sql_wrapper
import parameters

topic_display_names = parameters.topic_display_name()
topic_base_names = parameters.topic_base_name()

db = MS.connect(host="localhost", user = "root")
with db:
    cursor = db.cursor()
    sql = 'USE bubble'
    cursor.execute(sql)
      

app = Flask(__name__)
# TIP: set `app.debug = True`
# When you do so, changes in the file
# will reflect to your browser immediately
app.debug = True

@app.route("/")
def bubbleburst():
    topic_name = 'obamacare'
    state = 'ca'
    
    return render_template('bubbleburst_splash.html', results = None)
    
@app.route('/', methods=['POST'])
def search(): 
    inputs = request.form
    state = inputs['state']
    topic_display = inputs['topic']
    
    topic_name = topic_base_names[topic_display]
    bubble_type = inputs['bubble_type']
    if bubble_type == "Inside Bubble":
        table_type = "_favorites_algo2"
        bubble_display = "Inside"
    else:
        table_type = "_results_algo2"
        bubble_display = "Outside"   

    sql_query = "SELECT * "
    sql_query += "FROM " + topic_name + "_linkinfo, " + topic_name + table_type
    sql_query += " WHERE " + topic_name + table_type + ".story_id = " + topic_name + "_linkinfo.story_id"
    sql_query += " AND state = '" + state + "'"
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    
    dataset = []
    for r in rows:
        title = r[5]
        description = r[4]
        try:
            title.encode('utf-8')
        except:
            title = "No title available"
        try:
            description.encode('utf-8')
        except:
            description = "No description available"         
        row_dict = dict([('story_id', r[1]), ('title', title), ('description', description), ('url', r[6]), ('thumbnail_url', r[3]), ('clicks', r[7]), ('state_clicks', r[11])])
        if bubble_type == "Outside Bubble":
            row_dict['top_state'] = r[12]
            row_dict['top_state_clicks'] = r[13]
        dataset.append(row_dict)
    return render_template('bubbleburst_index.html', results = dataset, status = [parameters.state_display(state), topic_display, bubble_display])

@app.route('/about')
def about():
    return render_template('about.html')
    
@app.route('/contact')
def contact():
    return render_template('contact.html')
    
    
@app.route('/starter')
def starter(): 
    return render_template('index.html')

@app.route('/munter')
def munter():
    return render_template('bubbleburst_index_munter.html')
 
@app.route('/<pagename>')    
def regularpage(pagename=None):
    """
    Route not found by the other routes above.
    """
    return "You've arrived at " + pagename
        

if __name__ == "__main__":
    app.run()    