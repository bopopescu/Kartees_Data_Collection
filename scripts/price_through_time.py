# Function to get event data



from stubhub import *
import sys
sys.dont_write_bytecode = True

latest_game_schedules = '2017-03-05_18_29'


# 10 UTC = 5 PM EST, 4 PM PT
# 22 UTC = 5 AM EST, 5 AM PT

status_times = {1: [10,22],
                2: [10,16,22,4],
                3: [10,13,16,19,22,1,4,7],
                4: [11,12,13,14,15,16,17,18,19,20,21,22,23,1,2,3,4,5,6,7,8,9,10]
}

def get_status():
    
    start = datetime.datetime.utcnow()

    events=[]
    status = {}
    dates = {}
    teams = {}
    sports = {}

    now = datetime.datetime.utcnow()

    with open('../global_data/game_schedules/%s.csv' %latest_game_schedules, 'rU') as schedule_file:
        
        reader = csv.reader(schedule_file)
        schedule_file.readline()

        for row in reader:

            team = row[0]
            event = row[1]
            date = row[2]
            sport = row[3]

            event_date = dparser.parse(date).replace(tzinfo=None)
 
            date_dif = event_date - now

            time_dif = date_dif.days + (float(date_dif.seconds)/3600)/24
            
            dates[event] = date
            teams[event] = team
            sports[event] = sport


            events.append(event)
            
            if time_dif >30:
                status[event] = 1
            elif time_dif >15:
                status[event] = 2
            elif time_dif >7:
                status[event] = 3
            elif time_dif > 0:
                status[event] = 4
            # Else game must have been in the past
            else:
                status[event]=5

    current_hour= datetime.datetime.utcnow().strftime("%H")
    updated = []
    for event in events:
   
        if int(current_hour) in status_times[status[event]]:
           
            try:
                update_event_data(event, dates[event], teams[event], sports[event])
                updated.append(event)
               # pdb.set_trace()

                time.sleep(7)

            except Exception as e:
                logging.error(traceback.format_exc())

    end = datetime.datetime.utcnow()

    elapsed_minutes = float((end - start).seconds) / 60

    with open('../Collection_Logs.csv', 'a') as log_file:

        writer = csv.writer(log_file)
        writer.writerow([str(datetime.datetime.utcnow()), elapsed_minutes, updated])


def update_event_data(event_id, date, team, sport):

    now = datetime.datetime.utcnow()


    path = "../price_data/%s/%s.csv"%(team,event_id)

    include_header = False

    if os.path.isfile(path):

        file = open(path, 'a')

    else:
        directory ='../price_data/%s' %team

        if not os.path.exists(directory):
            os.makedirs(directory)

        file = open(path, 'wb') 

        include_header=True
        
    new_listings_request = stubhub.get_event_inventory(event_id)


    zones_dict, sections_dict, listings, metadata, event_id, total_tickets, average_price, wins, losses, l_10, opponent, total_listings = stubhub.get_event_data(event_id=event_id, new_listings=new_listings_request, sport=sport, team = team)


    listing_divisor = .181
     # Find section and zone averages
 #   total_value = 0
    zones_quant = {}
    zones_prices = {}
    zones_stats = {}
    section_prices = {}
    section_std = {}
 
    for listing in listings:
        if 'zoneId' in listing.keys():

            buyer_price = listing.get('currentPrice').get('amount')
        #    listing_price = round(buyer_price / (1+listing_divisor))
        #    listing_value = buyer_price * listing['quantity']

            zone_id = listing['zoneId']
    
            section_id = listing['sectionId']
     
            if zone_id not in zones_quant:
                zones_quant[zone_id] = [listing['quantity']]
                zones_prices[zone_id] = [buyer_price]
            else:
                zones_quant[zone_id].append(listing['quantity'])
                zones_prices[zone_id].append(buyer_price)
     
            if section_id not in section_prices:
                section_prices[section_id] = [buyer_price]
            else:
                section_prices[section_id].append(buyer_price)


            zone_count = np.sum(zones_quant[zone_id])
            zone_average = (np.dot(zones_quant[zone_id],zones_prices[zone_id]))/ zone_count
            zone_std = np.std(zones_prices[zone_id])
            zone_median = np.median(zones_prices[zone_id])

            section_std[section_id] = np.std(section_prices[section_id])
             
            zones_stats[zone_id] = [zone_average, zone_std, zone_count, zone_median]
    
    columns = []
    time_dif =  str(metadata['time_difference']).replace(",","")

   # Before season, ignore wins/losses and l_10

    if datetime.datetime.now()<datetime.datetime(2017,4,3):

        wins, losses, l_10 = 'NA', 'NA', 'NA'

   # Section = 1, Zone = 2


    for zone in zones_dict:

        zone_average, zone_std, zone_count, zone_median = zones_stats[zone][0],  zones_stats[zone][1],  zones_stats[zone][2], zones_stats[zone][3]

        column = [metadata['current_time']] + [time_dif] + [zone] + [zones_dict[zone]['zoneName']] + [total_tickets] + [average_price] +[zone_count]+ [zone_average]+[zones_dict[zone]['minTicketPrice']] +[zones_dict[zone]['maxTicketPrice']] + [zone_std] + [wins]+[losses]+[l_10] + [zone_median]+[total_listings] + [zones_dict[zone]['totalListings']]+[2] +[event_id]
        columns.append(column)
         
    for section in sections_dict:

        column = [metadata['current_time']] + [time_dif]+[section]+[sections_dict[section]['sectionName']] + [total_tickets] + [average_price] + [sections_dict[section]['totalTickets']] +[sections_dict[section]['averageTicketPrice']] +[sections_dict[section]['minTicketPrice']]+ [sections_dict[section]['maxTicketPrice']]+ [section_std[section]] +[wins]+[losses]+[l_10] + [np.median(section_prices[section])] + [total_listings] + [sections_dict[section]['totalListings']]+[1]+[event_id]
        columns.append(column)

    if include_header:

        header = ['Time','Time_Diff','Zone_Section_Id','Zone_Name','Total_Tickets','Average_Price','Zone_Section_Total_Tickets','Zone_Section_Average_Price','Zone_Section_Min_Price','Zone_Section_Max_Price','Zone_Section_Std','Wins','Losses','L_10','Section_Median','Total_Listings','Zone_Section_Num_Listings', 'Data_Type', 'Event_Id']
        
        csv.writer(file).writerow(header)

    csv.writer(file).writerows(columns)
     
    file.close()
 

if __name__ == '__main__':


    username = USERNAME
    password = PASSWORD
    basic_auth = BASIC_AUTH
    app_token = APP_TOKEN

    try:
        app_token = Stubhub.get_app_token(app_token=app_token)
        user_token = Stubhub.get_user_token(basic_auth=basic_auth, username=username, password=password)
        user_id = Stubhub.get_user_id(basic_auth=basic_auth, username=username, password=password)

        stubhub = Stubhub(app_token=app_token, user_token=user_token, user_id=user_id)

    
        get_status()

    except Exception as e:

        logging.error(traceback.format_exc())

