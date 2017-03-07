# Function to get Mets data from 2016 csvs and categorize by game tier

from stubhub import *
import shutil
import sys
import matplotlib.pyplot as plt
import random
sys.dont_write_bytecode = True

username = USERNAME
password = PASSWORD

basic_auth = BASIC_AUTH
#print basic_auth
app_token = APP_TOKEN

app_token = Stubhub.get_app_token(app_token=app_token)  

user_token = Stubhub.get_user_token(basic_auth=basic_auth, username=username, password=password)
user_id = Stubhub.get_user_id(basic_auth=basic_auth, username=username, password=password)

stubhub = Stubhub(app_token=app_token, user_token=user_token, user_id=user_id)
  


def get_game_tiers():

	base_url = 'https://kartees-api.mybluemix.net/api/v3'

	# Mets tead id here is 5649

	full_url = base_url + '/season/team/5649'

	headers = {'token':'login_2@b5e6c878ac62008e3f69329d222041b7','Content-Type':'application/json'}
	response = requests.get(full_url, headers = headers).json()

	 # Find season 47 - where we have data for 2016 season

	game_tiers = {}
	for doc in response:
		if doc["_id"] =="season_47":
			games = doc["games"]

			# Save the data in the game tiers dictionary
			for game in games:
				game_tiers[game["Event_Id"]]=game["game_value"]

	return game_tiers

def get_game_dates():

	base_url = 'https://kartees-api.mybluemix.net/api/v3'

	# Mets tead id here is 5649

	full_url = base_url + '/season/team/5649'

	headers = {'token':'login_2@b5e6c878ac62008e3f69329d222041b7','Content-Type':'application/json'}
	response = requests.get(full_url, headers = headers).json()

	 # Find season 47 - where we have data for 2016 season

	game_dates= {}
	for doc in response:
		if doc["_id"] =="season_47":
			games = doc["games"]

			# Save the data in the game tiers dictionary
			for game in games:
				#pdb.set_trace()
				#print game
				data = stubhub.get_event_data(game["Event_Id"], None)
				
				time.sleep(7)

				game_dates[game["Event_Id"]]=dparser.parse(data['event_date'])
				print game_dates

	return game_dates




def move_files(game_tiers):

	# Move the Mets games to their relative tier levels

	path = '/Users/mauricezeitouni/Desktop/Kartees_Data_Collection/2016_data'

	for game in game_tiers:

		src = '%s/all_games/%s.csv' %(path,game)

		dst = '%s/Mets/%s/%s.csv' %(path, game_tiers[game],game)

		shutil.move(src, dst)


# Function to go through each game, if there is data than append to all_data

# Adds the tier to the 18th column

def consolidate_data(game_tiers, last_row):

	# For the 5 good games, should have 164,000 rows of data
	
	path = '/Users/mauricezeitouni/Desktop/Kartees_Data_Collection/2016_data/Mets'

	# Create data matrix for new data set
	all_data = []

	for game in game_tiers:

		tier = game_tiers[game]

		new_path = '%s/%s/%s.csv' %(path, tier, game)

		# Check if this file has data
		if os.path.getsize(new_path)>0:

			# Open file and read data
			with open(new_path, 'rU') as game_file:

				reader = csv.reader(game_file)

				for row in reader:
					for i in range(0,last_row+1):
						if len(row) < i:
							row.append('MISSING')

					# Custom logic for this case - switch up rows
					if row[14] =="ZONE":
						value_15 = row[15]
						value_16 = row[16]
						value_17 = row[17]

						row[15] = value_17
						row[17] = value_16
						row[16] = value_15

					row.append(tier)

					all_data.append(row)


	return all_data

# Write all this data to new file
def write_to_file(all_data, header_row):

	timestamp = '{:%Y-%m-%d_%H_%M}'.format(datetime.datetime.utcnow())

	directory ='../2016_data/Mets/consolidated_datasets/%s' %timestamp

	os.makedirs(directory)

	with open('%s/all.csv' %directory, 'wb') as all_data_file:

		writer = csv.writer(all_data_file)

		writer.writerow(header_row)

		writer.writerows(all_data)

	with open('%s/sections.csv' %directory, 'wb') as section_data_file:

		writer = csv.writer(section_data_file)

		writer.writerow(header_row)

		for row in all_data:

			if row[14]== "SECTION":
				writer.writerow(row)

	with open('%s/zones.csv' %directory, 'wb') as zones_data_file:

		writer = csv.writer(zones_data_file)

		writer.writerow(header_row)

		for row in all_data:

			if row[14]== "ZONE":
				writer.writerow(row)


def convert_time_dif(old_path, new_path, header):

	rows = None

	with open(old_path,'rU' ) as read_file:

		reader = csv.reader(read_file)

		reader.next()

		rows = [row for row in reader]

		days = 0
		hours = 0

		for row in rows:

			time = row[1]

			# Convert this time to a float number

			if "days" in time:
				x = time.split('days')
				trimmed = [y.strip() for y in x]

				days = float(trimmed[0])
				time_obj = trimmed[1].split('.')[0]

				hours = float(time_obj.split(':')[0])
				minutes = float(time_obj.split(':')[1])

				hours = hours + (minutes/60)

				time_dif = days + (hours/24)

				row[1] = time_dif

			elif "day" in time:

				x = time.split('day')
				trimmed = [y.strip() for y in x]

				days = 1
				time_obj = trimmed[1].split('.')[0]

				hours = float(time_obj.split(':')[0])
				minutes = float(time_obj.split(':')[1])

				hours = hours + (minutes/60)

				time_dif = days + (hours/24)

				row[1] = time_dif

			else:

				days = 0
				time_obj = time.split('.')[0]

				hours = float(time_obj.split(':')[0])
				minutes = float(time_obj.split(':')[1])

				hours = hours + (minutes/60)

				time_dif = days + (hours/24)


				row[1] = time_dif

	with open(new_path, 'wb') as new_file:

		writer = csv.writer(new_file)

		writer.writerow(header)

		writer.writerows(rows)

def plot_time(path):

	with open(path, 'rU') as data_file:

		reader = csv.reader(data_file)
		reader.next()

		times = []
		x = []
		i = 0
		for row in reader:
			times.append(row[1])
			x.append(i)
			i+=1

		plt.plot(x,times)
		plt.show()


def convert_tier(old_path, new_path, header):

	rows = None

	tier_dict = {'marquee':1,
				'premium':2,
				'classic':3,
				'value':4,
				 'super_value':5}

	with open(old_path, 'rU') as old_file:

		reader = csv.reader(old_file)

		reader.next()

		rows = [row for row in reader]


		for row in rows:

			if row[14]=="SECTION": row[14] = 1
			if row[14]=="ZONE": row[14] = 2

			row[18] = tier_dict[str(row[18])]


	with open(new_path, 'wb') as new_file:

		writer = csv.writer(new_file)

		writer.writerow(header)

		writer.writerows(rows)


def input_future_prices(old_path, new_path, header):

	# n_days defines how many days ahead you want to find
	rows = None
	
	with open(old_path, 'rU') as old_file:

		reader = csv.reader(old_file)

		reader.next()

		rows = [row for row in reader]

	# Loop through and check the future price

	#days_list = [1 , 2, 3, 4, 8, 15, 22, 28, 32, 40, 50, 60]
	days_list = [1 , 2, 3, 4, 8, 15, 22]

	# rand_smpl = [ rows[i] for i in sorted(random.sample(xrange(len(rows)), 1000)) ]

	# rows = rand_smpl

	for n_days in days_list:

		index = days_list.index(n_days)
		
		if index==0:
			old_path = old_path
		else:
			old_path = '../2016_data/Mets/consolidated_datasets/2017-03-07_14_09/process_future_prices/%s.csv' %days_list[index-1]

		with open(old_path, 'rU') as old_file:

			reader = csv.reader(old_file)

			reader.next()

			rows = [row for row in reader]

			# if index==0:
			# 	rand_smpl = [ rows[i] for i in sorted(random.sample(xrange(len(rows)), 1000)) ]

			# 	rows = rand_smpl

			for row in rows:

				current_index = rows.index(row)

				percent_complete = (days_list.index(n_days))* (100/len(days_list)) + (float(current_index)/float(len(rows)) * (100/len(days_list)))

				print "%.3f" %percent_complete + "%%"

				current_zone = row[2]
				current_time_diff = float(row[1])

				#found = False
				k =1
				while k <(27000/130 and k<= len(rows)-1-current_index:

					# Check if this is our zone
					if rows[current_index+k][2]==current_zone:

						# Check if the difference in days matches
						if current_time_diff - float(rows[current_index+k][1])  >= n_days:

							next_price = rows[current_index+k][7]
							row.append(next_price)
							#found = True
							break

					k+=1

				# if not found: row.append('MISSING')

		new_path = '../2016_data/Mets/consolidated_datasets/2017-03-07_14_09/process_future_prices/%s.csv' %n_days

		with open(new_path, 'wb') as new_file:

			writer = csv.writer(new_file)

			writer.writerow(header)

			writer.writerows(rows)




if __name__ == '__main__':

	#game_tiers = get_game_tiers()

	#all_data = consolidate_data(game_tiers, 18)

	header_row = ['Time','Time_Diff','Zone','Zone_Name','Total_Tickets','Average_Price','Zone_Section_Total_Tickets','Zone_Section_Average_Price','Zone_Section_Min_Price','Zone_Section_Max_Price','Zone_Section_Std','Wins','Losses','L_10','Data_Type','Section_Median','Total_Listings','Zone_Section_Num_Listings', 'Tier']

	#write_to_file(all_data, header_row)

	old_path = '../2016_data/Mets/consolidated_datasets/2017-03-07_14_09/sections_final.csv'

	new_path = '../2016_data/Mets/consolidated_datasets/2017-03-07_14_09/process_future_prices/testing.csv' 

	#convert_time_dif(old_path, new_path, header_row)

	#plot_time(new_path)

	input_future_prices(old_path, new_path, header_row)

