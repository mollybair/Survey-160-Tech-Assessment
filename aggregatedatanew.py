## This line routes to the flask API. It's a fairly straightforward task with little opportunity to change
@app.route("/aggregatedata", methods=['GET', 'POST'])

## This is a helper function to retrieve aggregates from the database
def getaggregate(cursor, var_name, time_bool, table_name):
    ## If field is time related, enter this block
    if time_bool:
        ## Assume required aggregate is the average time
        ## Typecast to TIME to ensure field is stored as the correct data type for this operation 
        sql = 'SELECT AVERAGE(' + var_name + '::TIME) FROM' + table_name
        return cursor.execute(sql)

    ## If the field is not time related, enter this block
    else:
        ## 'SUM' can easily be replaced by another SQL aggregate function (such as MIN or MAX). The aggregate function can also be passed 
        ## as a parameter to make this helper function more generalizable 
        sql = 'SELECT SUM(' + var_name + ') FROM ' + table_name
        return cursor.execute(sql)

def aggregatedatanew():
    ## This is another straightforward line that I wouldn't change. This line retrieves the json data
    j = request.get_json()

    ## Lines 30, 34, and 38 --> I would split each of these lines into multiple lines, where the message is created on one line, and 
    ## then passed to logger.info(message)
    ## I would also consider creating a helper function since these lines are all very similar. The function would take userid, campaignid,
    ## or organizationid as a parameter. I think created a helper function here would be especially prudent if more id's were to be added.
    if ('userid' in j):

        app.logger.info(str(request.remote_addr)+' ['+str(datetime.datetime.now())+'] - INFO: aggregatedatanew (userid='+str(j['userid'])+')')

    elif ('campaignid' in j):

        app.logger.info(str(request.remote_addr)+' ['+str(datetime.datetime.now())+'] - INFO: aggregatedatanew (campaignid='+str(j['campaignid'])+')')

    elif ('organizationid' in j):

        app.logger.info(str(request.remote_addr)+' ['+str(datetime.datetime.now())+'] - INFO: aggregatedatanew (organizationid='+str(j['organizationid'])+')')

    ### Connect to a postgresql database.
    ## First establish the connection, using the kwargs for databse name, username used to authenticate, password used to authenticate, and
    ## the host the database is hosted on 
    conn = psycopg2.connect(
        database='my_database',
        user='postgres',
        password='password',
        host='localhost'
    )
    ## Then, create a cursor object
    cur = conn.cursor()

    ## This line calls another function and checks the validity
    ## If this function returns False, the connection to the database is closed
    if (not checkvalidjwt(cur,j)):

        conn.close()

        return Response(json.dumps({'success':'false'},default = myconverter),status=400,mimetype='application/json')

    ### You're creating a bunch of tasks to fulfill a request.

    ### You need aggregates for following fields: status, firstsms, lastsms, contact, start, engaged, morethanonehour, morethanonehourclosed

    ### Each aggregate should filter on userid, campaignid, and organizationid

    ### Please fill in using Python and SQL to the best of your ability. You can make up the schema names.

    ### HINT: How might your SQL be different for time related fields?

    ## Can only execute SQL commands if connection to databse is still open
    else:
        ## Create a dict, where the key is the field and the value is a list containing a bool for whether the field is a time related field, 
        ## and the name of the table the field is located in
        ## Without further documentation of the fields, I am assuming that firstsms, lastsms, and start are the only time related fields
        ## I am also assuming that these 3 fields are stored as hh:mm:ss date/time types
        var_dict = {'status': [False, user_info], 'firstsms': [True, sms_info], 'lastsms': [True, sms_info], 'contact': [False, user_info],
        'start': [True, user_info], 'engaged': [False, user_info], 'morethanonehour': [False, user_info], 'morethanonehourclosed': [False, user_info]}

        ## Loop through all the fields you need aggregates for 
        for var, values in var_dict.items():

            ## Split the dict values into two separate variables
            time_bool = values[0]
            table_name = values[1]

            ## Use an if-elif-elif block to filter on userid, campaignid, and organizationid
            ## Inside each block, define a variable name for each resulting aggregate
            ## Then, call a helper function to create the aggregate
            if ('userid' in j):
                agg_name = 'userid' + 'agg' + var
                agg_name = getaggregate(cur, var, time_bool, table_name)

            elif ('campaignid' in j):
                agg_name = 'campaignid' + 'agg' + var
                agg_name = getaggregate(cur, var, time_bool, table_name)

            elif ('organizationid' in j):
                agg_name = 'organizationid' + 'agg' + var
                agg_name = getaggregate(cur, var, time_bool, table_name)
