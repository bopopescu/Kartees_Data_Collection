import os


if __name__ == '__main__':

	now = datetime.datetime.utcnow()

	today_dir  = '../price_data/%s_%s_%s' %(now.year, now.month, now.day)


	# Check if path from today exists

	if os.path.exists(yesterday_dir):

		uploadDirectory(path_to_upload, '2017pricedata', yesterday_dir)

	upload_Collection_Logs()