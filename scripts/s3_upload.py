import boto3
import os
import datetime
import pdb
from cloudant.client import Cloudant
import smart_open
import csv
import shutil
from credentials import *


s3 = boto3.client('s3')

teams = {}

if 'VCAP_SERVICES' in os.environ:
	vcap = json.loads(os.getenv('VCAP_SERVICES'))

	print 'Found VCAP_SERVICES'

	if 'cloudantNoSQLDB' in vcap:
		creds = vcap['cloudantNoSQLDB'][0]['credentials']
		user = creds['username']
		password = creds['password']
		url = 'https://' + creds['host']
		client = Cloudant(user, password, url=url, connect=True)
else:
	client = Cloudant(CLOUDANT['username'], CLOUDANT['password'], url=CLOUDANT['url'],connect=True,auto_renew=True)

db = client['data_collection']

def cloudant_write_daily(new_data):


	current_week =  datetime.datetime.utcnow().isocalendar()[1]
	current_year = datetime.datetime.utcnow().year

	doc_id = "daily_upload_%s_week_%s" %(current_year,current_week)
	exists = False

	for doc in db:
		if doc['_id'] == doc_id:
			exists = True
			break
	
	if exists == False:

		db.create_document({"_id":"daily_upload_%s_week_%s" %(current_year,current_week),
							"logs":[]})
	
	logs_doc = db[doc_id]

	logs = logs_doc["logs"]

	logs.append(new_data)

	logs_doc.save()

def cloudant_write_total(new_data):

	current_week =  datetime.datetime.utcnow().isocalendar()[1]
	current_year = datetime.datetime.utcnow().year

	doc_id = "total_upload_%s_week_%s" %(current_year,current_week)

	exists = False
	for doc in db:
		if doc['_id'] == doc_id:
			exists = True
			break


	if exists == False:
	
		db.create_document({"_id":"total_upload_%s_week_%s" %(current_year,current_week),
							"logs":[]})
	
	logs_doc = db["total_upload_%s_week_%s" %(current_year,current_week)]

	logs = logs_doc["logs"]

	logs.append(new_data)

	logs_doc.save()



def uploadDirectory(path,bucketname, yesterday_dir):

    for root,dirs,files in os.walk(path):

    	for team in dirs:

    		for team_root, team_dirs, team_files in os.walk('%s/%s' %(path,team)):
    
    			for file in team_files:
    		
    				local_path = '%s/%s/%s' %(path,team, file)

    				s3.upload_file(local_path,bucketname,'daily/%s/%s/%s' %(yesterday_dir,team, file))

    				day_size = os.path.getsize(local_path)/1000

    				new_data = {

    						"UTC_Timestamp": str(datetime.datetime.utcnow()),
    						"Team_Uploaded": team,
    						"Event":file.replace('.csv',''),
    						"Day_Collected":yesterday_dir,
    						"Size_KB": day_size

    				}

    				cloudant_write_daily(new_data)
    				existing_lines = []
    				

		        	# Get new lines for daily

		        	if day_size >0:

			        	with open(local_path, 'rU') as new_data:

			        		reader = csv.reader(new_data)
			        		
			  
			        		reader.next()
			        		new_rows = [l for l in reader]
			
				        	new_rows = [[]]

			        	directory = '../price_data/tmp/%s' %(team)

			        	tmp_file_name = '%s/%s' %(directory,file)

			        	if not os.path.exists(directory):
			        		os.makedirs(directory)
			        		
			        	print 's3://2017pricedata/total/%s/%s' %(team,file)
			        	s3_client = boto3.client('s3')
			        	try:
							s3_client.download_file('2017pricedata', 'total/%s/%s'%(team,file),tmp_file_name)

							with open (tmp_file_name, 'a') as new_file:

								writer = csv.writer(new_file)

								writer.writerows(new_rows)
							
							size = os.path.getsize(tmp_file_name)/1000
							print 'Uploading file: %s - %s' %(team, file)
							
							s3_client.upload_file(tmp_file_name, '2017pricedata','total/%s/%s' %(team,file))

							shutil.rmtree(directory)

							new_doc = False


			        	except:

			        		with open (tmp_file_name, 'wb') as new_file:

								writer = csv.writer(new_file)

								header = ['Time','Time_Diff','Zone_Section_Id','Zone_Name','Total_Tickets','Average_Price','Zone_Section_Total_Tickets','Zone_Section_Average_Price','Zone_Section_Min_Price','Zone_Section_Max_Price','Zone_Section_Std','Win_PCT','Total_Games','L_10','Section_Median','Total_Listings','Zone_Section_Num_Listings', 'Data_Type', 'Event_Id']

								writer.writerow(header)

								writer.writerows(new_rows)

			        		print 'doesnt exist -starting new'
			        		
			        		s3.upload_file(local_path,bucketname,'total/%s/%s' %(team, file))
			        		size = os.path.getsize(tmp_file_name)/1000
			        		shutil.rmtree(directory) 

			        		new_doc =True


			        	new_data = {

	    						"UTC_Timestamp": str(datetime.datetime.utcnow()),
	    						"Team_Uploaded": team,
	    						"Event":file.replace('.csv',''),
	    						"Day_Added_To_Total":yesterday_dir,
	    						"Size_KB": size,
	    						"New_doc":new_doc

	    					}

	    				cloudant_write_total(new_data)



if __name__ == '__main__':

	now = datetime.datetime.utcnow()


	yesterday = now - datetime.timedelta(days=1)

	yesterday_dir  = '%s_%s_%s' %(yesterday.year, yesterday.month, yesterday.day)

	path_to_upload = '../price_data/%s' %yesterday_dir


	# Check if path from yesterday exists, and upload today's

	if os.path.exists(path_to_upload):


		uploadDirectory(path_to_upload, '2017pricedata', yesterday_dir)



		# new_data ={}
		# new_data[yesterday_dir]={}

		# for team in teams:

		# 	new_data[yesterday_dir][team] = teams[team]

		# cloudant_write(new_data)
		


