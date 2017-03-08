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
							row.append('NA')

					# Custom logic for this case - switch up rows
					if row[14] =="ZONE":
						value_15 = row[15]
						value_16 = row[16]
						value_17 = row[17]

						row[15] = value_17
						row[17] = value_16
						row[16] = value_15

					row.append(tier)
					row.append(game)

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


def factor_data(old_path, new_path, header):

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

def clean_l10(old_path, new_path, header):

	with open(old_path, 'rU') as old_file:

		reader = csv.reader(old_file)

		reader.next()

		rows = [row for row in reader]

		for row in rows:

			l_10 = str(row[13])

			try:
				wins = float(l_10[0])
				losses = float(l_10[-1])

				total = wins + losses

				pct = 0

				if total !=0: pct = float(wins/total)

				row[13] = pct

			except:
				row[13] = 'NA'

		with open(new_path, 'wb') as new_file:

			writer = csv.writer(new_file)

			writer.writerow(header)

			writer.writerows(rows)

def input_NA(old_path, new_path, header, row_length):

	with open(old_path, 'rU') as old_file:

		reader = csv.reader(old_file)

		reader.next()

		rows = [row for row in reader]

		for row in rows:

			for x in range(0,row_length-1):

				if len(row)<row_length:
					row.append('NA')

				if row[x] == 'MISSING' or row[x] == '' or row[x] == None:
					row[x] = 'NA'
				#row[:] = [x if (x != 'MISSING' and x !='') else 'NA' for x in row]


		with open(new_path, 'wb') as new_file:

			writer = csv.writer(new_file)

			writer.writerow(header)

			writer.writerows(rows)



def input_future_prices(old_path, header):

	rows = None
	
	with open(old_path, 'rU') as old_file:

		reader = csv.reader(old_file)

		reader.next()

		rows = [row for row in reader]

	# Loop through and check the future price

	#days_list = [1 , 2, 3, 4, 8, 15, 22, 28, 32, 40, 50, 60]

	size = int(raw_input("Size of days list array: "))
	days_list = []
	for n in range(0, size):
		days_list.append(int(raw_input("Number %s: " %(int(n)+1))))
	#days_list = [1 , 10, 30]

	for n in days_list:
		header.append('%s_days_ahead_price' %n) 

	# rand_smpl = [ rows[i] for i in sorted(random.sample(xrange(len(rows)), 1000)) ]

	# rows = rand_smpl

	for n_days in days_list:

		index = days_list.index(n_days)
		
		if index==0:
			old_path = old_path
		else:
			old_path = '../2016_data/Mets/consolidated_datasets/2017-03-08_08_47/process_future_prices/%s_days.csv' %days_list[index-1]

		with open(old_path, 'rU') as old_file:

			reader = csv.reader(old_file)

			reader.next()

			rows = [row for row in reader]

			# if index==0:
			#	pdb.set_trace()
				# rand_smpl = [ rows[i] for i in sorted(random.sample(xrange(len(rows)), 20000)) ]

				# rows = rand_smpl

			for row in rows:

				current_index = rows.index(row)

				percent_complete = (days_list.index(n_days))* (100/len(days_list)) + (float(current_index)/float(len(rows)) * (100/len(days_list)))

				print "%.3f" %percent_complete + "%%"

				current_zone = row[2]
				current_time_diff = float(row[1])
				current_event = row[19]

				k = 1

				append_value = 'NA'

				while k<= (len(rows)-1-current_index) and rows[current_index+k][19]==current_event:

					# Check if this is our zone
					if rows[current_index+k][2]==current_zone:
	
						# Check if the difference in days matches
						if current_time_diff - float(rows[current_index+k][1])  >= n_days:

							next_price = rows[current_index+k][7]
							append_value = next_price
							#found = True
							break
					k+=1

				# if append_value =='':
				# 	pdb.set_trace()
				# 	print rows.index(row)
				# 	print row
				row.append(append_value)

				# if not found: row.append('MISSING')

		new_path = '../2016_data/Mets/consolidated_datasets/2017-03-08_08_47/process_future_prices/%s_days.csv' %n_days

		with open(new_path, 'wb') as new_file:

			writer = csv.writer(new_file)

			writer.writerow(header)

			writer.writerows(rows)


def input_past_derivatives(old_path, header):

	rows = None
	
	with open(old_path, 'rU') as old_file:

		reader = csv.reader(old_file)

		reader.next()

		rows = [row for row in reader]


	size = int(raw_input("Size of past derivative list: "))
	derivative_list = []
	for n in range(0, size):
		derivative_list.append(float(raw_input("Number %s: " %(int(n)+1))))
	#days_list = [1 , 10, 30]

	for n in derivative_list:
		header.append('%s_past_days_price' %n)
		header.append('%s_past_days_derivative' %n) 
		header.append('%s_past_days_pctchange' %n)  

	for n_days in derivative_list:

		index = derivative_list.index(n_days)
		
		if index==0:
			old_path = old_path
		else:
			old_path = '../2016_data/Mets/consolidated_datasets/2017-03-08_08_47/process_past_derivatives/%s_days.csv' %derivative_list[index-1]
			#old_path = '../2016_data/Mets/consolidated_datasets/2017-03-08_08_47/process_past_derivatives/test_derivative.csv'
		

		with open(old_path, 'rU') as old_file:

			reader = csv.reader(old_file)

			reader.next()

			rows = [row for row in reader]

			#if index==0:
				#pdb.set_trace()
				# rand_smpl = [ rows[i] for i in sorted(random.sample(xrange(len(rows)), 10000)) ]

				# rows = rand_smpl

			for current_index,row  in reversed(list(enumerate(rows))):
				
				index_for_pct = len(rows)-current_index
				percent_complete = (derivative_list.index(n_days))* (100/len(derivative_list)) + (float(index_for_pct)/float(len(rows)) * (100/len(derivative_list)))

				print "%.3f" %percent_complete + "%%"

				current_zone = row[2]
				current_time_diff = float(row[1])
				current_event = row[19]
				current_price = float(row[7])

				#pdb.set_trace()
				#print current_zone
				#print current_time_diff
				#print current_event
				k = current_index

				last_price = 'NA'
				derivative = 'NA'
				pct_change = 'NA'

				while k>0 and rows[k-1][19]==current_event:

					# Check if this is our zone and differene in days matches
					if rows[k-1][2]==current_zone and float(rows[k-1][1]) -current_time_diff >=n_days :

							last_price = float(rows[k-1][7])
							# Derivative is difference in price over difference in time
							derivative = (current_price-last_price)/(current_time_diff-float(rows[k-1][1]))
							pct_change = ((current_price-last_price) / last_price)*100
							#append_value = last_price
							
							break
					k-=1

				row.append(last_price)
				row.append(derivative)
				row.append(pct_change)

				# if not found: row.append('MISSING')

		new_path = '../2016_data/Mets/consolidated_datasets/2017-03-08_08_47/process_past_derivatives/%s_days.csv' %n_days

		with open(new_path, 'wb') as new_file:

			writer = csv.writer(new_file)

			writer.writerow(header)

			writer.writerows(rows)


def check_rows():

	path = '../2016_data/Mets/consolidated_datasets/2017-03-08_08_47/process_past_derivatives'
	days_list = ['0.25','0.5','0.75',1,2,3,4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30]

	for file in days_list:
		
		with open('%s/%s_days.csv' %(path, file), 'rU') as data_file:

			reader = csv.reader(data_file)

			rows = [l for l in reader]

			print len(rows)

def consolidate_days():


	path ='../2016_data/Mets/consolidated_datasets/2017-03-08_08_47/process_past_derivatives'

	days_list = [4,8, 15, 40]

	rows = ''
	matrix = ''

	with open('%s/0.75_days.csv' %(path), 'rU') as first_file:

			reader = csv.reader(first_file)

			rows = [l for l in reader]

			matrix = np.array(rows)

	for file in days_list:
		
		with open('%s/%s_days.csv' %(path, file), 'rU') as data_file:

			reader = csv.reader(data_file)

			new_rows = [l for l in reader]

			#print len(new_rows[1])

			new_rows = np.array(new_rows)



			cols = new_rows[:, range(35,47)]
			
			#print cols

			matrix= np.hstack((matrix, cols))

			print "New Matrix size: %s" %str(matrix.shape)

	with open('%s/testing_stack.csv' %(path), 'wb') as new_file:

		writer = csv.writer(new_file)

		writer.writerows(matrix)

def count_NAs(path):

	with open(path, 'rU') as file:

		reader = csv.reader(file)

		rows = [row for row in reader]

		na_count= 0
		good_count = 0

		for row in rows:
			if "NA" in row:
				na_count+=1
			else:
				good_count+=1


		print "NA count: %s\n Good Rows: %s\n PCT bad: %s" %(na_count, good_count, (float(na_count)/float(na_count+good_count)))


if __name__ == '__main__':

	#game_tiers = get_game_tiers()

	#all_data = consolidate_data(game_tiers, 18)

	header_row = ['Time','Time_Diff','Zone','Zone_Name','Total_Tickets','Average_Price','Zone_Section_Total_Tickets','Zone_Section_Average_Price','Zone_Section_Min_Price','Zone_Section_Max_Price','Zone_Section_Std','Wins','Losses','L_10','Data_Type','Section_Median','Total_Listings','Zone_Section_Num_Listings','Tier','Event_Id','1_days_ahead_price','2_days_ahead_price','3_days_ahead_price','4_days_ahead_price','5_days_ahead_price','6_days_ahead_price','7_days_ahead_price','8_days_ahead_price','9_days_ahead_price','10_days_ahead_price','12_days_ahead_price','15_days_ahead_price','20_days_ahead_price','25_days_ahead_price','30_days_ahead_price','40_days_ahead_price']
	#write_to_file(all_data, header_row)

	old_path = '../2016_data/Mets/consolidated_datasets/2017-03-08_08_47/sections_future_past_days.csv'

	#new_path = '../2016_data/Mets/consolidated_datasets/2017-03-08_08_47/zones_clean_time_factored_l10_na.csv' 

	#convert_time_dif(old_path, new_path, header_row)

	#factor_data(old_path, new_path, header_row)

	#clean_l10(old_path, new_path, header_row)

	#input_NA(old_path, new_path, header_row)

	#plot_time(new_path)

	#input_future_prices(old_path, header_row)

	#input_past_derivatives(old_path, header_row)

	#check_rows()
	#consolidate_days()

	count_NAs(old_path)

