import boto3
import os
import datetime
import pdb
import botocore
import pandas as pd
import smart_open
import csv
import requests
import shutil

s3_resource = boto3.resource('s3')

def consolidate_dailys(day):

	bucket = s3_resource.Bucket('2017pricedata')

	total_size = 0

	for obj in bucket.objects.all():
		
		key = obj.key

		if 'daily' in key and 'csv' in key:

			key_list = key.split('/')
		#	pdb.set_trace()

			file_day = key_list[1]
			team = key_list[2]
			event_csv = key_list[3]

			if file_day == day:

				lines = []
				#print 's3://2017pricedata/daily/%s/%s/%s' %(day,team,event_csv)
				first_line = True
				for line in smart_open.smart_open('s3://2017pricedata/daily/%s/%s/%s' %(day,team,event_csv)):
					
					if not first_line:
						lines.append(line.replace('\r\n','').split(','))
					else:
						first_line= False

				total_size += append_to_total(event_csv, lines, team)

	return total_size


def append_to_total(event_csv, lines, team):

	bucket = s3_resource.Bucket('2017pricedata')

	import boto.s3.connection
	access_key = 'AKIAIOWD5SFYZNZTNPWA'
	secret_key = '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'

	conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
      #  host = 'objects.dreamhost.com',
        #is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

	bucket = conn.get_bucket('2017pricedata')

	key = 'total/%s/%s' %(team,event_csv)
	exists = False
	existing_lines = []
	key_file = None


	for obj in bucket.list():
		
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

		elif 'total' in obj.key:

			key_file = bucket.new_key(key)

			existing_lines = list(lines)

			break

		else:
			print 'not our total file - %s' %obj.key



	# Save to tmp csv
	directory = '../price_data/tmp/%s' %(team)

	tmp_file_name = '%s/%s' %(directory,event_csv)

	os.makedirs(directory)

	with open (tmp_file_name, 'wb') as new_file:

		writer = csv.writer(new_file)

		writer.writerows(existing_lines)

	size = os.path.getsize(tmp_file_name)/1000

	key_file.set_contents_from_filename(tmp_file_name)

	shutil.rmtree(directory) 

	return size


if __name__ == '__main__':

	now = datetime.datetime.utcnow()

	days_to_collect = []

	for i in range(1,8):

		day = now - datetime.timedelta(days=i)
		days_to_collect.append('%s_%s_%s' %(day.year, day.month, day.day))

	sizes = {}
	total_size = 0
	days_consolidated = []
	collection_file_path = '../weekly_consolidation_log.csv'

	with open(collection_file_path, 'rU') as log_file:

		reader = csv.reader(log_file)
		reader.next()
		

		for row in reader:

			days_list = eval(row[1])

			for day in days_list:
				days_consolidated.append(day)

	for day in reversed(days_to_collect):

		print day

		if day not in days_consolidated:
			
			size = consolidate_dailys(day)

			total_size+=size

			sizes[day] = size

			print sizes[day]


	with open(collection_file_path, 'a') as log_file:

		writer = csv.writer(log_file)

		writer.writerow([now, days_to_collect,total_size,sizes ])


