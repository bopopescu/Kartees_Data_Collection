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

		# Create teams array and id dict
		teams = []	
		team_ids = {}
		team_dates = {}

		# Read team_names csv and get names

		with open('../global_data/team_names.csv', 'rU') as team_names:

			reader = csv.reader(team_names)

			for row in reader:
				teams.append(row[0])

			print "Successfully read all teams from team_names"

		# Loop through each team to get their events
		for team in teams:

			try:

				# Get games from stubhub
				events = stubhub.get_team_games(team)['events']

				# Logic to filter out non-home games such as away or spring training games

				ids =[]
				venues = []
				dates = []
				for event in events:
					venues.append(event['venue']['name'])

				home_field = max(set(venues), key=venues.count)

				for event in events:
					if event['venue']['name'] == home_field:
						ids.append(event['id'])
						dates.append(str(dparser.parse(event['eventDateUTC'])))

				team_ids[team] = ids
				team_dates[team] = dates

				print "Successfully retrieved games for: %s" %team

			except:

				print "Error retrieving games for: %s" %team

			# Sleep for 7 seconds so we don't go over 10 calls/minute

			time.sleep(7)

		# Get timestamp to use as file name

		timestamp = '{:%Y-%m-%d_%H_%M}'.format(datetime.datetime.utcnow())

		# Write new file with the ids

		with open('../global_data/game_ids/%s.csv'%timestamp, 'wb') as games_file:

			writer = csv.writer(games_file)
			writer.writerow(['Team Name', 'Num Ids', 'Id Array','Dates Array'])

			for team in teams:

				writer.writerow([team] + [len(team_ids[team])] + [team_ids[team]]+[team_dates[team]])

		# Write to game schedules file
		with open('../global_data/game_schedules/%s.csv' %timestamp, 'wb') as schedule_file:

			writer = csv.writer(schedule_file)

			writer.writerow(['Team', 'Event Id', 'Event Time'])

			# Loop through each team
			for team in teams:

				# Loop through each game for that team
				for game_id in team_ids[team]:

					# The date is the corresponding element of id, but in the teamd dates arrar
					game_date = team_dates[team][team_ids[team].index(game_id)]

					# Write the game date in the csv file
					writer.writerow([team, game_id, game_date])

	except Exception as e:

		logging.error(traceback.format_exc())