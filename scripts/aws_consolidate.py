import boto3
import os
import datetime
import pdb
import botocore
import pandas as pd
import smart_open
import csv

s3_resource = boto3.resource('s3')

def add_to_total(day):

	
	bucket = s3_resource.Bucket('2017pricedata')

	for obj in bucket.objects.all():
		
		key = obj.key

		if 'daily' in key and 'csv' in key:

			key_list = key.split('/')
			pdb.set_trace()
			print key_list
			file_day = key_list[1]
			team = key_list[2]
			event_csv = key_list[3]

			if file_day == day:

				lines = []

				for line in smart_open.smart_open('s3://2017pricedata/daily/%s/%s/%s' %(day,team,event_csv)):
					

					lines.append(line.replace('\r\n','').split(','))

					append_to_total(event_csv, lines, team, day)


def append_to_total(event_csv, lines, team, day):

	lines = []

	bucket = s3_resource.Bucket('2017pricedata')


	for line in smart_open.smart_open('s3://2017pricedata/total/%s/%s' %(day,team,event_csv)):
		

		lines.append(line.replace('\r\n','').split(','))


    # for root,dirs,files in os.walk(path):

    # 	for team in dirs:

    # 		for team_root, team_dirs, team_files in os.walk('%s/%s' %(path,team)):
    
    # 			for file in team_files:
    		
    # 				local_path = '%s/%s/%s' %(path,team, file)
    				
		  #       	s3.upload_file(local_path,bucketname,'daily/%s/%s/%s' %(yesterday_dir,team, file))


if __name__ == '__main__':

	now = datetime.datetime.utcnow()

	days_to_collect = []

	for i in range(1,8):

		day = now - datetime.timedelta(days=i)
		days_to_collect.append('%s_%s_%s' %(day.year, day.month, day.day))

	for day in days_to_collect:

		add_to_total(day)

	# yesterday_dir  = '%s_%s_%s' %(yesterday.year, yesterday.month, yesterday.day)

	# path_to_upload = '../price_data/%s' %yesterday_dir

	# Check if this path exists, and upload it

	# if os.path.exists(path_to_upload):

	# 	uploadDirectory(path_to_upload, '2017pricedata', yesterday_dir)