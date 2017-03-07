# Function to get schedule of all events in the game_ids file

# Takes input of the name of game_id file to read

# Loops through each Id, finds UTC date of event

# Writes to game_schedules folder



from stubhub import *
import sys
sys.dont_write_bytecode = True
import os.path
import ast


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

		# Ask user for file name
		file_name = str(raw_input("Enter game id file name (without .csv extension): "))

		path = '../global_data/game_ids/%s.csv' %file_name

		# Create dict to store team id data

		ids = {}

		# Check if this file exists
		if os.path.isfile(path):

			# Open the file and read the data
			with open(path, 'rU') as id_file:

				reader = csv.reader(id_file)

				# Skip header row
				id_file.readline() 

				# Read through each row and populate id dict
				for row in reader:

					team_name = row[0]
					ids[team_name] = ast.literal_eval(row[2])

			# Get timestamp to use as file name

			timestamp = '{:%Y-%m-%d_%H_%M}'.format(datetime.datetime.utcnow())

			# For each event get the game date
			with open('../global_data/game_schedules/%s.csv' %timestamp, 'wb') as schedule_file:

				writer = csv.writer(schedule_file)

				writer.writerow(['Team', 'Event Id', 'Event Time'])

				# Loop through each team
				for team in ids:

					# Loop through each game for that team
					for game_id in ids[team]:

						game_date = stubhub.get_game_date(game_id)

						# Write the game date in the csv file
						writer.writerow([team, game_id, str(game_date)])



		else:
			print 'That Game ID file does not exist, exiting script' 
		
		


	except Exception as e:

		logging.error(traceback.format_exc())