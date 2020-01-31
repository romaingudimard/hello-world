from slackclient import SlackClient
import pandas as pd
import datetime
import gspread
import json
import mysql.connector
from oauth2client.service_account import ServiceAccountCredentials
import json
import time

########################################################################################################################################
# Open files with credentials
########################################################################################################################################

with  open('credentials.json') as json_file:
    db_credentials = json.load(json_file)

with  open('slack_credentials.json') as json_file:
    slack_credentials = json.load(json_file)

########################################################################################################################################
# Description
########################################################################################################################################

'''This program has been created to alert if a given store has more orders than it can manage. If it does,
a message is sent to a Slack channel and a team will change the radius to 0 prevent that store of having
even more orders. Also it sends a message if the radius of the store is 0 and it does have a number of orders
that can be managed.
For that, the program gets the information of the database, puts it into a file and merges it with another
file that contains some static information about the store, like the default radius or the thresholds.
After that, we evaluate if the store has the conditions to set the radius to 0 to avoid receiving more
orders or to set the radius to the default value and continue receiving orders.
Finally, in case that a store is in one of the previous cases, it sends a message to Slack to manually set the
value of its radius to 0 or to the original one.'''

########################################################################################################################################
# Open files with credentials
########################################################################################################################################

# connecting database

try:
    conn = mysql.connector.connect(database= db_credentials['database'], user = db_credentials['user'], password = db_credentials['password'],
                              host = db_credentials['host'], port = db_credentials['port'])
except Exception as e:
    raise Exception('Unable to connect to the database, check password please')

cursor = conn.cursor()
    
# Accesses spreadsheets

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("service-account-file.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_key('1Z_WpFok-4GzPyIFq7yB1I6zsliplvSNL2WLGjIJpAtA').sheet1  # open the spreadsheet
sc = SlackClient(slack_credentials['token_glovo'])
scl = SlackClient(slack_credentials['token_glovo_latam'])

########################################################################################################################################
# Send the messages to the Slack channel
########################################################################################################################################

def slack_message_close_store(sc, slack_channel, url, store_address_id):

    sc.api_call(
    "chat.postMessage",
    channel=str(slack_channel),
    username='Store Saturation',
    icon_emoji=':rabbit2:',
    text=("Please set the radius of the store address " + '<' + str(url) + '|' + str(store_address_id) + '>' + " to 0m.")
  )

def slack_message_open_store(sc, slack_channel, url, store_address_id, store_distance):

    sc.api_call(
    "chat.postMessage",
    channel=str(slack_channel),
    username='Store Saturation',
    icon_emoji=':rabbit2:',
    text=("Please set the radius of the store address " + '<' + str(url) + '|' + str(store_address_id) + '>' + " to "+str(store_distance)+"m.")
  )

def slack_message_wrong_radius(sc, slack_channel, url, store_address_id, store_distance):

    sc.api_call(
    "chat.postMessage",
    channel=str(slack_channel),
    username='Wrong Radius',
    icon_emoji=':fearful:',
    text=("The radius of the store address " + '<' + str(url) + '|' + str(store_address_id) + '>' + " is not "+str(store_distance)+"m. Please set the radius of the store to "+str(store_distance)+"m.")
  )

########################################################################################################################################
# Main
########################################################################################################################################

def main():
    start_t = time.time()
    partner_saturation = sheet.get_all_values()
    del partner_saturation[0]
    stores = ""
    for partner in partner_saturation:
        if partner[2] != "":
            if stores != "":
                stores = stores + ", '" + str(partner[2]) + "'"
            else:
                stores = stores + "'" + str(partner[2]) + "'"
    if stores == "":
        return(None)
    query = """select count(o.id)                                 as Orders,
       o.store_address_id                          as Store_address_id_2,
       sadz.maximum_delivery_distance_meters       as Store_distance_2
from orders o
join store_address_delivery_zone sadz on sadz.store_address_id = o.store_address_id
where o.store_address_id in (""" + stores + """)
and o.store_address_id is not null
and o.current_status_type in ('NewStatus', 'ProgressStatus')
and TIMESTAMPDIFF(HOUR, o.activation_time, now()) < 24
group by o.store_address_id;"""
    # Running the query
    cursor.execute(query)
    stores_datas = cursor.fetchall()
    #Add hyperlinks to the admin
    url0 = 'https://beta-admin.glovoapp.com/store/'
    url1 = '/address/'
    for store in stores_datas:
        active_orders = store[0]
        store_address_id = store[1]
        actual_store_distance = int(store[2])
        now = datetime.datetime.now()
        for i in range (0,len(partner_saturation)):
            store2 = partner_saturation[i]
            if str(store2[2]) == str(store_address_id) and store2[10] != 'No':
                if now > (datetime.datetime.strptime(store2[9], '%Y-%m-%d %H:%M:%S') + datetime.timedelta(seconds=int(store2[6]))) or store2[9] == '':
                    store_distance = int(store2[5])
                    max_store = store2[3]
                    min_store = store2[4]
                    store_id = store2[1]
                    slack_channel = store2[7]
                    url = url0 + str(store_id) + url1 + str(store_address_id)
                    if store2[8] == 'Glovo_main':
                        channel = sc
                    if store2[8] == 'latam':
                        channel = scl
                    if int(actual_store_distance) == 0 and int(active_orders) < int(min_store):
                        slack_message_open_store(channel, slack_channel, url, store_address_id, store_distance)
                        sheet.update_cell(i+2, 10, str(now))
                    elif int(active_orders) > int(max_store) and int(actual_store_distance) != 0:
                        slack_message_close_store(channel, slack_channel, url, store_address_id)
                        sheet.update_cell(i+2, 10, str(now))  
                    elif int(actual_store_distance) != int(store_distance) and int(actual_store_distance) != 0:
                        slack_message_wrong_radius(channel, slack_channel, url, store_address_id, store_distance)
                        sheet.update_cell(i+2, 10, str(now))
    print('Success of the execution:    ', str(datetime.datetime.now()))
        
main()




