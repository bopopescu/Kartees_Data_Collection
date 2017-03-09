import boto3
import os
import datetime
import pdb
s3 = boto3.client('s3')


def uploadDirectory(path,bucketname, yesterday_dir):

    for root,dirs,files in os.walk(path):
    	for team in dirs:

    		for team_root, team_dirs, team_files in os.walk('%s/%s' %(path,team)):
    
    			for file in team_files:
    		
    				local_path = '%s/%s/%s' %(path,team, file)
    				
		        	s3.upload_file(local_path,bucketname,'%s/%s/%s' %(yesterday_dir,team, file))


if __name__ == '__main__':

	now = datetime.datetime.utcnow()

	yesterday = now - datetime.timedelta(days=1)

	yesterday_dir  = '%s_%s_%s' %(yesterday.year, yesterday.month, yesterday.day)

	path_to_upload = '../price_data/%s' %yesterday_dir

	# Check if this path exists, and upload it

	if os.path.exists(path_to_upload):

		uploadDirectory(path_to_upload, '2017pricedata', yesterday_dir)