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

schedule={"Baltimore Orioles":"00:00",
"Los Angeles Dodgers":"00:30",
"Washington Nationals":"01:00",
"Arizona Diamondbacks":"01:30",
"San Diego Padres":"02:00",
"Toronto Blue Jays":"02:30",
"Chicago Cubs":"03:00",
"Colorado Rockies":"03:30",
"Houston Astros":"04:00",
"Milwaukee Brewers":"04:30",
"Cleveland Indians":"05:00",
"Kansas City Royals":"05:30",
"St. Louis Cardinals":"06:00",
"Chicago White Sox":"06:30",
"Boston Red Sox":"07:00",
"Seattle Mariners":"07:30",
"Philadelphia Phillies":"08:00",
"New York Mets":"08:30",
"Atlanta Braves":"09:00",
"San Francisco Giants":"09:30",
"Oakland Athletics":"10:00",
"New York Yankees":"10:30",
"Miami Marlins":"11:00",
"Cincinnati Reds":"11:30",
"Minnesota Twins":"12:00",
"Detroit Tigers":"12:30",
"Los Angeles Angels":"13:00",
"Texas Rangers":"13:30",
"Pittsburgh Pirates":"14:00",
"Tampa Bay Rays":"21:00"}

def consolidate_dailys(s3_resource,day, use_team):

	bucket = s3_resource.Bucket('2017pricedata')

	total_size = 0


	for obj in bucket.objects.all():
		
		key = obj.key

		if 'daily' in key and 'csv' in key:

			key_list = key.split('/')

			file_day = key_list[1]
			team = key_list[2]
			event_csv = key_list[3]

			if file_day == day and use_team==team :

				lines = []
				#print 's3://2017pricedata/daily/%s/%s/%s' %(day,team,event_csv)
				first_line = True
				for line in smart_open.smart_open('s3://2017pricedata/daily/%s/%s/%s' %(day,team,event_csv)):
					
					if not first_line:
						lines.append(line.replace('\r\n','').split(','))
					else:
						first_line= False


				total_size += append_to_total(s3_resource,event_csv, lines, team)
				print '--------------%s - %s, SIZE: %s--------------' %(day,event_csv, total_size)

	return total_size


def append_to_total(s3_resource, event_csv, lines, team):

	bucket = s3_resource.Bucket('2017pricedata')

	key = 'total/%s/%s' %(team,event_csv)
	exists = False
	existing_lines = []
	key_file = None

	for obj in bucket.objects.all():
		
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

		else:

			existing_lines = list(lines)

			#break

		# else:
		# 	print 'not our total file - %s' %obj.key



	# Save to tmp csv
	directory = 'price_data/tmp/%s' %(team)

	tmp_file_name = '%s/%s' %(directory,event_csv)

	os.makedirs(directory)

	with open (tmp_file_name, 'wb') as new_file:

		writer = csv.writer(new_file)

		existing_lines[0] = ['Time','Time_Diff','Zone_Section_Id','Zone_Name','Total_Tickets','Average_Price','Zone_Section_Total_Tickets','Zone_Section_Average_Price','Zone_Section_Min_Price','Zone_Section_Max_Price','Zone_Section_Std','Win_PCT','Total_Games','L_10','Section_Median','Total_Listings','Zone_Section_Num_Listings', 'Data_Type', 'Event_Id']

		writer.writerows(existing_lines)

	size = os.path.getsize(tmp_file_name)/1000

	s3_client = boto3.client('s3')

	print 'Uploading file: %s - %s' %(team, event_csv)
	s3_client.upload_file(tmp_file_name, '2017pricedata','total/%s/%s' %(team,event_csv))

	shutil.rmtree(directory) 

	return size

def cloudant_write(new_data):

	logs = logs_doc["logs"]

	logs.append(new_data)

	logs_doc.save()


def aws_consolidate(client, first_day, last_day):

	hour = datetime.datetime.utcnow().hour
	minute = datetime.datetime.utcnow().minute
	
	for team in schedule:
		if team == "Tampa Bay Rays":
			print int(schedule[team].split(":")[0])
			print minute ==int(schedule[team].split(":")[1])
		
	
		#print int(schedule[team].split(":")[1]) 
		if int(schedule[team].split(":")[0])==hour and int(schedule[team].split(":")[1])==minute:
			print 'here'
			use_team = team

			db = client['data_collection']
			logs_doc = db["weekly_logs"]

			logs = logs_doc["logs"]

			now = datetime.datetime.utcnow()

			s3_resource = boto3.resource('s3')

			days_to_collect = []

			for i in range(int(first_day),int(last_day)):

				day = now - datetime.timedelta(days=i)
				days_to_collect.append('%s_%s_%s' %(day.year, day.month, day.day))


			sizes = {}
			total_size = 0
			days_consolidated = []
			collection_file_path = '../weekly_consolidation_log.csv'


			if logs:
				for log in logs:

					days = log['Days_Collected']

					for day in days:
						days_consolidated.append(day)

			print "Days to collect: %s" %days_to_collect


			for day in reversed(days_to_collect):

				print day

				if day not in days_consolidated:

					size = consolidate_dailys(s3_resource,day, use_team)

					total_size+=size

					sizes[day] = size

					print sizes[day]


			elapsed = "%.2f" %float(float((datetime.utcnow() - now).seconds) /60)

			data = {"Timestamp":str(now),
					"Days_Collected":days_to_collect,
					"Total_storage_KB":total_size,
					"Sizes":sizes,
					"Time_Elapsed":elapsed}


			cloudant_write(data)

