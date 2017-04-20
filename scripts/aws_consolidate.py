import boto3
import os
import datetime
import pdb
import botocore
import smart_open
import csv
import requests
import shutil
from cloudant.client import Cloudant


schedules = []

# schedules.append({
# "San Diego Padres":"00:00",
# "Toronto Blue Jays":"03:00",
# "Chicago Cubs":"06:00",
# "Colorado Rockies":"09:00",
# "Houston Astros":"12:00",
# "Milwaukee Brewers":"15:00",
# "Cleveland Indians":"18:00",
# "Kansas City Royals":"21:00"
# })

# schedules.append({ 
# "Philadelphia Phillies":"00:00",
# "New York Mets":"03:00",
# "Atlanta Braves":"06:00",
# "San Francisco Giants":"09:00",
# "Oakland Athletics":"12:00",
# "New York Yankees":"15:00",
# "Miami Marlins":"18:00",
# "Cincinnati Reds":"21:00"
# })

# schedules.append({ 
# "St. Louis Cardinals":"00:00",
# "Minnesota Twins":"03:00",
# "Detroit Tigers":"06:00",
# "Los Angeles Angels":"09:00",
# "Texas Rangers":"12:00",
# "Pittsburgh Pirates":"15:00",
# "Tampa Bay Rays":"18:00"
# })

# schedules.append({ 
# "Seattle Mariners":"00:00",
# "Chicago White Sox":"03:00",
# "Boston Red Sox":"06:00",
# "Baltimore Orioles":"09:00",
# "Los Angeles Dodgers":"12:00",
# "Washington Nationals":"15:00",
# "Arizona Diamondbacks":"18:00"
# })


def consolidate_dailys(s3_resource,day, use_team):

	bucket = s3_resource.Bucket('2017pricedata')

	total_size = 0

	pre = 'daily/%s/%s' %(day, use_team) 

	for obj in bucket.objects.filter(Prefix=pre):
		
		key = obj.key

		print "Observing key %s"%key

		if 'daily' in key and 'csv' in key and day in key and use_team in key:

			key_list = key.split('/')

			file_day = key_list[1]
			team = key_list[2]
			event_csv = key_list[3]

			#if file_day == day and use_team==team :

			lines = []
			print "Getting lines for %s, %s" %(team, day)
			first_line = True
			for line in smart_open.smart_open('s3://2017pricedata/daily/%s/%s/%s' %(day,team,event_csv)):
				
				if not first_line:
					lines.append(line.replace('\r\n','').split(','))
				else:
					first_line= False

			print lines[0]
			
			total_size += append_to_total(s3_resource,event_csv, lines, team)
			print '--------------%s - %s, SIZE: %s--------------' %(day,event_csv, total_size)

	return total_size


def append_to_total(s3_resource, event_csv, lines, team):

	bucket = s3_resource.Bucket('2017pricedata')

	key = 'total/%s/%s' %(team,event_csv)

	pre = 'total/%s' %(team) 

	exists = False
	existing_lines = []
	key_file = None

	for obj in bucket.objects.filter(Prefix=pre):
		
		if obj.key == key:

			exists = True

			# Grab current 'total data'
			for x in smart_open.smart_open('s3://2017pricedata/total/%s/%s' %(team,event_csv)):

				existing_lines.append(x.replace('\r\n','').split(','))

			# Append the newest data from this day

			for row in lines:

				existing_lines.append(row)

			key_file = obj

			break

	
	if not exists:

		existing_lines = list(lines)



	# Save to tmp csv
	directory = 'price_data/tmp/%s' %(team)

	tmp_file_name = '%s/%s' %(directory,event_csv)

	os.makedirs(directory)

	with open (tmp_file_name, 'wb') as new_file:

		writer = csv.writer(new_file)

		header = ['Time','Time_Diff','Zone_Section_Id','Zone_Name','Total_Tickets','Average_Price','Zone_Section_Total_Tickets','Zone_Section_Average_Price','Zone_Section_Min_Price','Zone_Section_Max_Price','Zone_Section_Std','Win_PCT','Total_Games','L_10','Section_Median','Total_Listings','Zone_Section_Num_Listings', 'Data_Type', 'Event_Id', 'Weekend', 'Day_Game']

		if not exists:
			writer.writerow(header)

		writer.writerows(existing_lines)

	size = os.path.getsize(tmp_file_name)/1000

	print size

	s3_client = boto3.client('s3')

	print 'Uploading file: %s - %s' %(team, event_csv)
	s3_client.upload_file(tmp_file_name, '2017pricedata','total/%s/%s' %(team,event_csv))
	
	shutil.rmtree(directory) 

	return size

def cloudant_write(logs_doc, new_data):

	logs = logs_doc["logs"]

	logs.append(new_data)

	logs_doc.save()


def aws_consolidate(creds, first_day, last_day, schedule_type):

	hour = datetime.datetime.utcnow().hour
	minute = datetime.datetime.utcnow().minute

	print hour
	print minute

	client = Cloudant(creds['user'],creds['password'] , url=creds['url'],connect=True,auto_renew=True)

	db = client['data_collection']

	worker_schedules = db['consolidation_schedule']['workers_array']

	schedule = worker_schedules[schedule_type]

	print schedule

	for team in schedule:

		if int(schedule[team].split(":")[0])==hour and int(schedule[team].split(":")[1])==minute:

			print team

			use_team = team

			
			logs_doc = db["weekly_logs"]

			logs = logs_doc["logs"]

			days_already_consolidated = []

			if logs:
				for log in logs:

					if log['Team'] == team:
						days = log["Days_Collected"]

						for day in days:
							days_already_consolidated.append(str(day))


			now = datetime.datetime.utcnow()

			s3_resource = boto3.resource('s3')

			days_to_collect = []


			for i in range(int(first_day),int(last_day)):

				day = now - datetime.timedelta(days=i)
				day = '%s_%s_%s' %(day.year, day.month, day.day)


				if not days_already_consolidated or str(day) not in days_already_consolidated:
					days_to_collect.append(day)
				


			print "Days to collect: %s" %days_to_collect

			sizes = {}
			total_size = 0
			
			elapsed = "%.2f" %float(float((datetime.datetime.utcnow() - now).seconds) /60)

			data = {"Timestamp":str(now),
					"Days_Collected":days_to_collect,
					"Team":use_team,
					"Total_storage_KB":total_size,
					"Sizes":sizes,
					"Time_Elapsed":elapsed}


			
			for day in reversed(days_to_collect):

				print "Consolidating day: %s " %day

				size = consolidate_dailys(s3_resource,day, use_team)

				total_size+=size

				sizes[day] = size

				print sizes[day]


			elapsed = "%.2f" %float(float((datetime.datetime.utcnow() - now).seconds) /60)

			data = {"Timestamp":str(now),
					"Days_Collected":days_to_collect,
					"Team":use_team,
					"Total_storage_KB":total_size,
					"Sizes":sizes,
					"Time_Elapsed":elapsed}


			cloudant_write(logs_doc, data)

def remove_spaces():

	print 'here'
	s3_resource = boto3.resource('s3')

	bucket = s3_resource.Bucket('2017pricedata')

	directory = ""	
	event_csv = ""

	for obj in bucket.objects.all():

		key = obj.key

		#print "Observing key %s"%key

		if 'total' in key and 'csv' in key:

			key_list = key.split('/')

			print key_list
			
			#file_day = key_list[1]
			team = key_list[1]
			event_csv = key_list[2]

			print key_list

			directory = '../price_data/tmp_spaces/%s' %(team)
			
			print directory
			tmp_file_name = "%s/%s" %(directory,event_csv)
			

        	# if not os.path.exists(directory):
			os.makedirs(directory)
        		
			print 's3://2017pricedata/total/%s/%s' %(team,event_csv)
			s3_client = boto3.client('s3')

			s3_client.download_file('2017pricedata', 'total/%s/%s'%(team,event_csv),tmp_file_name)

			with open(tmp_file_name, 'rU') as old_file:

				reader = csv.reader(old_file)

				rows = [l for l in reader]

			new_dir = '%s/new' %(directory)
			os.makedirs(new_dir)
			new_file_path = '%s/%s' %(new_dir, event_csv)
			with open(new_file_path, 'wb') as new_file:

				writer = csv.writer(new_file)

				for row in rows:

					if row != []:

						writer.writerow(row)

			s3_client.upload_file(new_file_path, '2017pricedata','total/%s/%s' %(team,event_csv))
			shutil.rmtree(directory)





if __name__=='__main__':

	client = Cloudant(CLOUDANT['username'], CLOUDANT['password'], url=CLOUDANT['url'],connect=True,auto_renew=True)

	aws_consolidate(client,1,4,0)







