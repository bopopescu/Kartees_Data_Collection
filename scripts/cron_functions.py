import csv
import pdb
import time

def get_cron(account):

	path = '../global_data/cron_%s.csv' %account

	with open(path, 'rU') as cron_file:

		reader = csv.reader(cron_file)

		header = reader.next()

		rows = [l for l in reader]

		return rows, header


def cron_write_delay(account):

	path = '../global_data/cron_%s.csv' %account

	delay = 0

	rows, header = get_cron(account)

	next_value = 11

	for row in rows:

		if row[1] == "NA":
			next_value = row[0]
			break

	if next_value == 11:

		now = time.time()
		first_time = float(rows[0][1])

		difference = now - first_time


		if difference<59:

			delay = float(60) - difference


			time.sleep(delay)

		with open(path, 'wb') as cron_file:

			writer = csv.writer(cron_file)

			writer.writerow(header)

			for row in rows:

				if int(row[0])==1:

					writer.writerow([row[0], time.time()])

				else:

					writer.writerow([row[0], 'NA'])

	else:

		with open(path, 'wb') as cron_file:

			writer = csv.writer(cron_file)

			writer.writerow(header)

			for row in rows:

				if row[0] == next_value:

					now = time.time()
					writer.writerow([next_value,now])

				else:
					writer.writerow(row)

if __name__ == '__main__':

	cron_write_delay("LABO")

