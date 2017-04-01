import boto3
import os
import datetime
import pdb
from cloudant.client import Cloudant


s3 = boto3.client('s3')

teams = {}

def cloudant_write(new_data):

	if 'VCAP_SERVICES' in os.environ:

    vcap = json.loads(os.getenv('VCAP_SERVICES'))
    print('Found VCAP_SERVICES')
    if 'cloudantNoSQLDB' in vcap:
        creds = vcap['cloudantNoSQLDB'][0]['credentials']
        user = creds['username']
        password = creds['password']
        url = 'https://' + creds['host']
        client = Cloudant(user, password, url=url, connect=True)
  
	else:

		client = Cloudant(CLOUDANT['username'], CLOUDANT['password'], url=CLOUDANT['url'],connect=True,auto_renew=True)
	

	db = client['data_collection']
	logs_doc = db["daily_upload"]

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

		        	if team not in teams.keys():
		        		teams[team] =[]
		        		
		        	teams[team].push(file)



if __name__ == '__main__':

	now = datetime.datetime.utcnow()

	yesterday = now - datetime.timedelta(days=1)

	yesterday_dir  = '%s_%s_%s' %(yesterday.year, yesterday.month, yesterday.day)

	path_to_upload = '../price_data/%s' %yesterday_dir

	# Check if path from yesterday exists, and upload today's

	if os.path.exists(path_to_upload):

		uploadDirectory(path_to_upload, '2017pricedata', yesterday_dir)



		new_data ={}
		new_data[yesterday_dir]={}

		for team in teams:

			new_data[yesterday_dir][team] = teams[team]

		cloudant_write(new_data)
		


