import requests
import csv
import json
import datetime
import time
from static_dictionaries import get_market_cap, get_sport, get_team, get_performer_id
import xml.etree.ElementTree as ET
import numpy as np
#from scipy.stats import norm, mode
from datetime import timedelta
import espn
import traceback
import logging
import os.path
import dateutil.parser as dparser
# from django.utils.timezone import utc
import pdb
from credentials import *

import sys

sys.dont_write_bytecode = True

# try:
#     import http.client as http_client
# except ImportError:
#     # Python 2
#     import httplib as http_client
# http_client.HTTPConnection.debuglevel = 1

# # You must initialize logging, otherwise you'll not see debug output.
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

cache = {}


def req(full_url, headers, params, req_type='GET', use_cache=True):
    cache_key = str(full_url) + str(headers) + str(params) + str(req_type)
    if use_cache and (cache_key in cache):
        return cache[cache_key]
    #print "Request:", full_url, headers, params, req_type
    response = None
    
    if req_type == 'GET':
        response = requests.get(full_url, headers=headers, params=params)
    elif req_type == 'POST':
        response = requests.post(full_url, headers=headers, params=params)
    elif req_type == 'PUT':
        #import pdb; pdb.set_trace()
        response = requests.put(full_url, headers=headers, data=json.dumps(params))
    elif req_type == 'DELETE':
        response = requests.delete(full_url, headers=headers, params=params)
    else:
        return None
    #pdb.set_trace()
    #print response.json()
   # import pdb; pdb.set_trace()
    cache[cache_key] = response
    return cache[cache_key]


def login(account):
    basic_auth, username, password, base_url, client_id, client_secret = CREDS[account]['BASIC_AUTH'],CREDS[account]['USERNAME'], CREDS[account]['PASSWORD'], URL, CREDS[account]['CLIENT_ID'], CREDS[account]['CLIENT_SECRET']
    headers = {'Content-Type': 'application/x-www-form-urlencoded',
               'Authorization': 'Basic %s' % (basic_auth),
               }

    params = {
        'grant_type': 'password',
        'username': username,
        'client_secret': client_secret,
        'client_id': client_id,
        'password': password,
        'scope': 'PRODUCTION'
    }
   
    return req(full_url='%s/login' % (base_url), headers=headers, params=params, req_type='POST')


class Stubhub():
    @staticmethod
    def get_app_token(account):
        return CREDS[account]['APP_TOKEN']

    @staticmethod
    def get_user_token(account):
        basic_auth, username, password, base_url=CREDS[account]['BASIC_AUTH'],CREDS[account]['USERNAME'], CREDS[account]['PASSWORD'], URL
        return login(account=account).json()[
            'access_token']

    @staticmethod
    def get_user_id(account):
        basic_auth, username, password, base_url=CREDS[account]['BASIC_AUTH'],CREDS[account]['USERNAME'], CREDS[account]['PASSWORD'], URL
        return login(account=account).headers[
            'X-StubHub-User-GUID']

    def __init__(self, account):

        app_token = Stubhub.get_app_token(account)  
        user_token = Stubhub.get_user_token(account)
        user_id = Stubhub.get_user_id(account)

        self.app_token = app_token
        self.user_token = user_token
        self.user_id = user_id

    def send_req(self, url, token_type='USER', req_type='GET', headers=None, params=None, use_cache=False):
        token = self.user_token if token_type == 'USER' else self.app_token
        params = params or {}
        headers = headers or {'Authorization': 'Bearer %s' % (token), 'Content-Type': 'application/json'}

        full_url = '%s%s' % (URL, url)
        # print full_url, params, headers

        return req(full_url=full_url, headers=headers, params=params, req_type=req_type, use_cache=use_cache)

    def get_event(self, event_id):
        return self.send_req('/catalog/events/v2/%s' % (event_id), token_type='APP')

    def get_event_inventory(self, event_id, start=0, rows=10000, zonestats=True, sectionstats=True):
        params = {'eventId': event_id, 'start': start, 'rows': rows, 'zonestats' : zonestats, 'sectionstats' : sectionstats}
        return self.send_req('/search/inventory/v2', token_type='APP', req_type='GET', params=params).json()

    def get_listing_data(self, listing_id):
        return self.send_req('/inventory/listings/v1/%s' % (listing_id), token_type='APP')

    def change_price(self, listing_id, new_price):
        #listing = {'listing': {} }
        #params = listing['listing']['pricePerTicket'] =  new_price
        #print listing
        params = {"listing": {'pricePerTicket': new_price}}
        return self.send_req('/inventory/listings/v1/%s' % (listing_id), token_type='USER', req_type='PUT', params=params).text

    # This function doesnt work - I need it to return a list of games for that team
    # API found here - https://developer.stubhub.com/store/site/pages/doc-viewer.jag?category=Search&api=EventSearchAPIv2&endpoint=searchforeventsv2&version=v2
    def get_team_games(self, team):
        #performer_id = get_performer_id(event_id)
        
        params = {'name': team, 'parking': False, 'start': 0, 'limit':500}
        return self.send_req('/search/catalog/events/v3', token_type='APP',req_type='GET', params=params).json()

    def get_game_date(self, event_id):
    
        # Get XML of event details
        event_details = self.get_event(event_id=event_id).text
        root = ET.fromstring(event_details)
        #root = ET.fromstring(unicode(event_details.decode('utf-8')))
        event_date_UTC_unformatted = root[7].text
        event_date_UTC = dparser.parse(event_date_UTC_unformatted)
       # event_date_UTC = datetime.datetime.strptime(event_date_UTC_unformatted,  "%b %d %Y")
        
        return event_date_UTC

    def check_date(self, event_id):
    
        # Get XML of event details
        event_details = self.get_event(event_id=event_id).text
        root = ET.fromstring(event_details)
        #root = ET.fromstring(unicode(event_details.decode('utf-8')))
        event_date_UTC_unformatted = root[7].text
        event_date_UTC = datetime.datetime.strptime(event_date_UTC_unformatted[:10] , '%Y-%m-%d')
    
        now = datetime.datetime.now()
    
        if event_date_UTC > now:
            return True
        else:
            return False

    def get_event_data(self, event_id, sport, team, new_listings = None):
        # Get XML of event details
        event_details = self.get_event(event_id=event_id).text

        #import pdb; pdb.set_trace()
        #print event_details
        #root = ET.fromstring(event_details)
        event_details = event_details.encode('utf-8', 'ignore')
        event_details = event_details.decode('ascii', 'ignore')
        root = ET.fromstring(unicode(event_details.encode('utf-8')))
        #print root[7].text
        event_date_UTC_unformatted = root[7].text
        event_date_UTC = dparser.parse(event_date_UTC_unformatted).replace(tzinfo=None)
        #event_date_UTC = datetime.datetime.strptime(event_date_UTC_unformatted[:16] , '%y-%M-%d:%H:%M')
        #print event_date_UTC
        #current_time = datetime.datetime.now(datetime.timezone.utc)
       
        for elem in root.iter(tag='secondaryName'):
            opponent = elem.text
        
        now = datetime.datetime.utcnow()
        
        date_dif = event_date_UTC - now

        time_difference_in_days = date_dif.days + (float(date_dif.seconds)/3600)/24

      
        current_time_formatted = now.strftime("%Y-%m-%d %H:%M:%S")
        event_date_formatted = event_date_UTC.strftime("%Y-%m-%d %H:%M:%S")
        metadata = {'event_date':event_date_formatted, 'event_id' : event_id, 'time_difference' : time_difference_in_days, 'current_time':current_time_formatted}


        if new_listings !=None:

            data = new_listings

            sections_dict = {section['sectionId'] : section for section in data['section_stats']} 

            zones_dict = {zone['zoneId'] : zone for zone in data['zone_stats']}
            listings = data['listing']
            total_tickets = data['totalTickets']
            average_price = data['pricingSummary']['averageTicketPrice']
            total_listings = data['totalListings']
            
            # Get team info
            wins, losses, l_10 = espn.get_team_performance(event_id, sport, team)
    
            return zones_dict, sections_dict, listings, metadata, event_id, total_tickets, average_price, wins, losses, l_10, opponent, total_listings
        
        else:
            return metadata
            
if __name__ == '__main__':

    old_time = datetime.datetime.now()

    username = USERNAME
    password = PASSWORD
    
    basic_auth = BASIC_AUTH
    #print basic_auth
    app_token = APP_TOKEN
    
    
    try:

        stubhub = Stubhub(account = 'LABO')
   
        #event_list = [9370813, 9370773, 9370849, 9370785, 9370651, 9370664, 9370688, 9370708, 9370659, 9371397, 9371402, 9371410, 9371429, 9371138, 9371158, 9371165, 9371178, 9371146, 9371157, 9371167, 9371186, 9371201, 9341475, 9341486, 9341506, 9341526, 9341540, 9341429, 9341444, 9341472, 9341496, 9341636, 9341669, 9341682, 9341695, 9342365, 9342374, 9342393, 9342407, 9342419, 9342412, 9342418, 9342424, 9342429, 9342434]
        
        # event = stubhub.get_event(9710889)
        # print event.text
        games = stubhub.get_event_inventory(9710889)
        print games
     
        #keys = games['events'][0].keys()
     
        # #pdb.set_trace()


        # url = 'https://cdcde260-6832-446f-955e-28af0193fe6a-bluemix:c22f953dd39391d9e1fe1ebc77835baccce56378ffbb48c3bb7cae936d5785d2@cdcde260-6832-446f-955e-28af0193fe6a-bluemix.cloudant.com/kartees'
        # cloudant_data = requests.get('%s/_all_docs?include_docs=true' %url).json()
        # cloudant_data = cloudant_data['rows']
        # seasons = []
        # for doc in cloudant_data:
        #    # print doc
        #     if "season" in doc["id"]:
        #         seasons.append(doc)

        # mets = []
        # i = 0
        # for season in seasons:
        #     #print i
        #     if str(season['doc']['games'][0]['Home_Team']) == "5649":
        #         mets.append(season)
        #     i+=1

        # #print mets[0]['id']
        # dates_dict = {}
        # dates_list = []
        # for game in mets[0]['doc']['games']:

        #     dates_dict[game['Game_Date_Time']] = game['Event_Id']
        #     dates_list.append(game['Game_Date_Time'])
        #     # if str(game['Game_Date_Time']) == "2016-04-08":
        #     #     opener = game

        # dates_list.sort()
        
        # values = ['Marquee','Classic','Value','Super_Value','Super_Value','Super_Value','Super_Value','Super_Value','Super_Value','Value','Classic','Classic','Super_Value','Super_Value','Super_Value','Value','Value','Value','Classic','Classic','Classic','Classic','Classic','Classic','Value','Value','Value','Value','Value','Value','Classic','Premium','Premium','Classic','Classic','Value','Premium','Premium','Premium','Classic','Classic','Classic','Classic','Premium','Premium','Premium','Classic','Classic','Classic','Classic','Premium','Premium','Premium','Marquee','Marquee','Classic','Classic','Classic','Premium','Premium','Premium','Premium','Premium','Premium','Classic','Classic','Classic','Classic','Classic','Classic','Classic','Value','Classic','Classic','Super_Value','Super_Value','Super_Value','Super_Value','Value','Classic','Classic']

        # api_url = 'https://kartees-api.mybluemix.net/api/v3/event'
        # headers = {'token':'login_2@b5e6c878ac62008e3f69329d222041b7',
        #             'Content-Type':'application/json'

        # }
        # i = 0
        # for date in dates_list:
        #     url = '%s/%s/%s' %(api_url,dates_dict[date] ,values[i].lower())

        #     response = requests.put(url, headers=headers)

        #     print response.text
        #     i=i+1
        # events = [9445120,9445123,9445125,9445126,9445128,9445036,9445134,9445135,9445136,9445137,9445139,9445141,9445143,9445144,9445146,9445039,9445148,9445151,9445152,9445153,9445027,9445156,9445041,9445030,9445093,9445032,9445033,9445164,9445037,9445167,9445169,9445170,9445171,9445044,9445173,9445174,9445175,9445176,9445177,9445178,9445181,9445182,9445055,9445057,9445059,9445061,9445062,9445063,9445065,9445047,9445068,9445069,9445071,9445074,9445155,9445077,9445049,9445080,9445028,9445084,9445085,9445086,9445087,9445088,9445089,9445090,9445091,9445092,9445158,9445094,9445095,9445106,9445075,9445108,9445109,9445111,9445112,9445114,9445172,9445116,9445118]
        # #event = stubhub.get_event_data()

        # dates_dict = {}
        # dates_list = []

        # for event in events[0:3]:

        #     data = stubhub.get_event(event).text
        #     print data
        #     date = data['event_date']
        #     dates_dict[date] = data
        #     dates_list.append(date)
        #     time.sleep (7)

        # dates_list.sort()

        # print dates_dict[dates_list[0]]
            

        # team = "Chicago Cubs"
        
        # events = stubhub.get_team_games(team)['events']
        # ids =[]
        # venues = []
        # for event in events:
        #     venues.append(event['venue']['name'])

        # home_field = max(set(venues), key=venues.count)
        
        # for event in events:
        #     if event['venue']['name'] == home_field:
        #         ids.append(event['id'])
        
        # print (ids)
        # print len(ids)
#         
#         listing = 1177028278
#          
#         print stubhub.change_price(listing, "70")
#          
#         #print keys
#       #  for i in range (0,200):
#        #     print str(games['events'][i]['id']) + ": " +str(games['events'][i]['name'])+ ": " +str(games['events'][i]['venue']['address1'])
#         # nfl list   9454368, 9298537, 9454719, 9298616, 9454374, 9298597, 9454380, 9298655, 9454369]
#         #event_list = [9445134, 9445028, 9445151, 9445089, 9445062, 9370813]
#          
#         # #test_event_list = [9454374, 9298597, 9454380, 9298655, 9454369]
#         #Run our events, but time delay after 10 requests
#         counter = 0
#         for event in EVENT_LIST:
#              
#             try:
#                 if stubhub.check_date(event):
#                     new_listings_request = stubhub.handle_sold(event)
#                     event_data = stubhub.get_event_data(event_id=event, new_listings = new_listings_request)
#                     stubhub.write_event_data_to_csv(event_data=event_data)
#                 else:
#                     print "Event %s passed" %event
#             except Exception as e:
#                 logging.error(traceback.format_exc())
#                  
#                  
#             if counter == 8:
#                time.sleep(60)
#                counter =0
#             counter+=1
#        
#         #Use this code to see how many trues came out
#         counter=0
#         trues = 0
#         for event in event_list:
#             with open("csvs/master/%s.csv" %(event), "rU") as csvfile:
#                 reader = csv.reader(csvfile)
#                 for row in reader:
#                     counter+=1
#                     #print row[26]
#                     if row[34]=="True":
#                         trues+=1
#        
#         #testing cron job
#        
#         with open("output.csv", 'a') as outputcsv:
#             x = csv.writer(outputcsv)
#             time = datetime.datetime.now()
#             elapsed = time - old_time
#             x.writerow([time, trues, counter, str(divmod(elapsed.total_seconds(),60)[0]) + ' minutes, ', str(divmod(elapsed.total_seconds(),60)[1]) + ' seconds'])
#      
    except Exception as e:
        logging.error(traceback.format_exc())
