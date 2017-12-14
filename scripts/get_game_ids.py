# Function to update the event schedules for all teams

# Opens 'team_names' csv to get all teams

# Calls stubhub to get all game ids for that team

# Filters out spring training and away games

# Sleeps 7 seconds so as not to go over 10 calls/minute

# Writes the result to new csv file in games ids folder

# Also writes to game schedule folder

from stubhub import *
import sys
sys.dont_write_bytecode = True
import pdb

if __name__ == '__main__':


	sport = sys.argv[1]

	username = USERNAME
	password = PASSWORD
	basic_auth = BASIC_AUTH
	app_token = APP_TOKEN

	try:
		stubhub = Stubhub(account="LABO")
		# Create teams array and id dict
		teams = []	
		team_ids = {}
		team_dates_utc = {}
		team_dates_local = {}
		performer_ids = {}
		cities = {}
		names={}

		# Read team_names csv and get names

		with open('../global_data/team_names.csv', 'rU') as team_names:

			reader = csv.reader(team_names)

			reader.next()

			for row in reader:
				if row[0]==sport:
					team = row[1]+" "+row[2]
					teams.append(team)
					cities[team] = row[1]
					names[team]=row[2]
			print "Successfully read all teams from team_names"

		# Loop through each team to get their events
		for team in teams:

			try:

				# Get games from stubhub
				data = stubhub.get_team_games(team)
			
				events = data['events']

				# Logic to filter out non-home games such as away or spring training games

				ids =[]
				venues = []
				dates_utc = []
				dates_local = []
				performer_id=[]
				for event in events:
					venues.append(event['venue']['name'])

				home_field = max(set(venues), key=venues.count)

				for event in events:
					if event['venue']['name'] == home_field:
						try:
							performer_id.append(event['performers'][0]['id'])
						except:
							print('couldnt find performer')
						ids.append(event['id'])
						dates_utc.append(str(dparser.parse(event['eventDateUTC'])))
						dates_local.append(str(dparser.parse(event['eventDateLocal'])))
				team_ids[team] = ids
				team_dates_utc[team] = dates_utc
				team_dates_local[team] = dates_local
				performer_ids[team] = max(set(performer_id), key=performer_id.count)
				
				print "Successfully retrieved games for: %s" %team

			except:

				print "Error retrieving games for: %s" %team

			# Sleep for 7 seconds so we don't go over 10 calls/minute

			time.sleep(7)

		# Get timestamp to use as file name

		timestamp = '{:%Y-%m-%d_%H_%M}'.format(datetime.datetime.utcnow())

		# Write new file with the ids

		# with open('../global_data/game_ids/%s.csv'%timestamp, 'wb') as games_file:

		# 	writer = csv.writer(games_file)
		# 	writer.writerow(['Sport', 'Team Name', 'Num Ids', 'Id Array','Dates Array'])

		# 	for team in teams:

		# 		writer.writerow([sport] + [team] + [len(team_ids[team])] + [team_ids[team]]+[team_dates[team]])

		# Write to game schedules file
		with open('../global_data/game_schedules/%s/%s.csv' %(sport,timestamp), 'wb') as schedule_file:

			writer = csv.writer(schedule_file)

			writer.writerow(['TeamId', 'TeamCity','TeamName', 'Event Id', 'Event Date UTC', 'Event Date Local'])

			# Loop through each team
			for team in teams:

				# Loop through each game for that team
				for game_id in team_ids[team]:

					# The date is the corresponding element of id, but in the teamd dates arrar
					game_date_utc = team_dates_utc[team][team_ids[team].index(game_id)]
					game_date_local = team_dates_local[team][team_ids[team].index(game_id)]
					# Write the game date in the csv file
					writer.writerow([performer_ids[team], cities[team], names[team], game_id, game_date_utc,game_date_local ])

	except Exception as e:

		logging.error(traceback.format_exc())