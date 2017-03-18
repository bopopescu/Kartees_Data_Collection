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




if __name__ == '__main__':

	now = datetime.datetime.utcnow()

	s3_resource = boto3.resource('s3')
	s3_client = boto3.client('s3')


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
