import numpy as np
import csv

def validate_data():

	with open('../Collection_Logs.csv', 'rU') as file:

		reader = csv.reader(file)

		reader.next()

		rows = [l for l in reader]

	past_rows_elapsed = {}
	past_rows_elapsed['7-15'] = []
	past_rows_elapsed['15+'] = []
	past_rows_elapsed['0-7'] = []

	for row in rows:

		log_type = str(row[3])

		if rows.index(row)==len(rows)-1:
			last_row = row
			last_row_value = float(row[1])
		else:
			past_rows_elapsed[log_type].append(float(row[1]))

	
	last_row_type = str(row[3])


	if last_row_value > (np.mean(past_rows_elapsed[last_row_type]) +  np.std(past_rows_elapsed[last_row_type]) *1.95) or last_row_value < (np.mean(past_rows_elapsed[last_row_type]) -  np.std(past_rows_elapsed[last_row_type])*1.95):

		print 'different'

	else:
		print 'good'

if __name__ == '__main__':

	validate_data()
