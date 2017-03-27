# Function to get event data



from stubhub import *
from cron_functions import *
import sys
import time
sys.dont_write_bytecode = True

latest_game_schedules = '2017-03-05_18_29'


def update_event_data(event_id, team, sport):

    first = time.time()

    now = datetime.datetime.utcnow()

    directory = '../price_data/%s_%s_%s' %(now.year, now.month, now.day)

    if not os.path.exists(directory): os.makedirs(directory)


    path = "%s/%s/%s.csv"%(directory,team,event_id)

    include_header = False

    if os.path.isfile(path):

        file = open(path, 'a')

    else:
        new_team_directory ='%s/%s' %(directory, team)

        if not os.path.exists(new_team_directory):
            os.makedirs(new_team_directory)

        file = open(path, 'wb') 

        include_header=True

    second = time.time()
        
    new_listings_request = stubhub.get_event_inventory(event_id)

    third = time.time()

    zones_dict, sections_dict, listings, metadata, event_id, total_tickets, average_price, wins, losses, l_10, opponent, total_listings = stubhub.get_event_data(event_id=event_id, new_listings=new_listings_request, sport=sport, team = team)

    fourth = time.time()

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

    win_pct = 0
    total_games = 0

    if wins != "NA":
        win_pct = float(float(wins)/float(float(wins)+float(losses)))
        total_games = int(wins) + int(losses)

    # Section = 1, Zone = 2

    for zone in zones_dict:

        zone_average, zone_std, zone_count, zone_median = zones_stats[zone][0],  zones_stats[zone][1],  zones_stats[zone][2], zones_stats[zone][3]

        column = [metadata['current_time']] + [time_dif] + [zone] + [zones_dict[zone]['zoneName']] + [total_tickets] + [average_price] +[zone_count]+ [zone_average]+[zones_dict[zone]['minTicketPrice']] +[zones_dict[zone]['maxTicketPrice']] + [zone_std] + [win_pct]+[total_games]+[l_10] + [zone_median]+[total_listings] + [zones_dict[zone]['totalListings']]+[2] +[event_id]
        columns.append(column)
         
    for section in sections_dict:

        column = [metadata['current_time']] + [time_dif]+[section]+[sections_dict[section]['sectionName']] + [total_tickets] + [average_price] + [sections_dict[section]['totalTickets']] +[sections_dict[section]['averageTicketPrice']] +[sections_dict[section]['minTicketPrice']]+ [sections_dict[section]['maxTicketPrice']]+ [section_std[section]] +[win_pct]+[total_games]+[l_10] + [np.median(section_prices[section])] + [total_listings] + [sections_dict[section]['totalListings']]+[1]+[event_id]
        columns.append(column)

    if include_header:

        header = ['Time','Time_Diff','Zone_Section_Id','Zone_Name','Total_Tickets','Average_Price','Zone_Section_Total_Tickets','Zone_Section_Average_Price','Zone_Section_Min_Price','Zone_Section_Max_Price','Zone_Section_Std','Win PCT','Total_Games','L_10','Section_Median','Total_Listings','Zone_Section_Num_Listings', 'Data_Type', 'Event_Id']
        
        csv.writer(file).writerow(header)

    csv.writer(file).writerows(columns)
     
    file.close()
    
    fifth = time.time()


if __name__ == '__main__':

    try:

        # Get account to use from first command line arg
        first= time.time()
        account = sys.argv[1]
        stubhub = Stubhub(account)

        # Get game to use from second command line arg
        event_id = sys.argv[2]
        team = sys.argv[3]
        sport = sys.argv[4]
        
        cron_write_delay(account)
        
        update_event_data(event_id, team, sport)
        second = time.time()

        print 'true'

    except Exception as e:

        print 'false'

        #logging.error(traceback.format_exc())

