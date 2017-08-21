#from lxml import etree
import urllib
from bs4 import BeautifulSoup
import csv
import stubhub
import static_dictionaries
import datetime
import pdb
import time


def get_team_performance(sport, team):

	first = time.time()
	current_sport = sport
	full_team_name = team

	sports = ['nba','mlb','nhl','nfl']
	#sports = ['mlb']
	stats_row_buffer = {'mlb': 13, 'nba': 14, 'nhl': 16, 'nfl': 13}

	sports_wins = {}
	sports_losses = {}
	sports_l_10 = {}

	if sport !='nhl':

		second = time.time()
		web_page = urllib.urlopen("http://espn.go.com/%s/standings" % sport).read()
		third = time.time()
		soup = BeautifulSoup(web_page, "html.parser")

		teams = soup.find_all("span", class_= "team-names")
		stats = soup.find_all("td")

		#pdb.set_trace()
		wins = {}
		losses = {}
		l_10 = {}

		k = 1
		i=0
		for team in teams:

		 	wins[team.text] = stats[k-i].text
		 	losses[team.text] = stats[k-i+1].text

		 	if sport =='nba':
		 		l_10[team.text] = stats[k+12].text
		 	elif sport == 'mlb':
		 		l_10[team.text] = stats[k+10-i].text
		 		i+=1

		 	k += stats_row_buffer[sport]

		sports_wins[sport] = wins
		sports_losses[sport] = losses
		sports_l_10[sport] = l_10

		fourth = time.time()

		#print '2 to 3: %s' %str(third-second)
		#print '3 to 4: %s' %str(fourth-third)

	else:
		web_page = urllib.urlopen("http://www.msn.com/en-us/sports/nhl/standings").read()
		soup = BeautifulSoup(web_page, "html.parser")
		wins = {}
		losses = {}
		l_10 = {}
		teams= soup.find_all('td', class_="teamname")
		short_names_classes = soup.find_all('tr', class_="rowlink")
	#	print teams
		for team in short_names_classes:
			tds = team.find_all('td')

			name = tds[1].text.replace(" - x","").replace(" - y","").replace(" - z","")

			wins[name] = tds[4].text
			losses[name] = tds[5].text
			l_10[name] = tds[12].text

		sports_wins['nhl'] = wins
		sports_losses['nhl'] = losses
		sports_l_10['nhl'] = l_10

	print sports_wins
	wins = sports_wins[current_sport][full_team_name]
	losses = sports_losses[current_sport][full_team_name]
	l_10 = str(sports_l_10[current_sport][full_team_name]).replace("-"," ")

	denom = (float(l_10[0])+ float(l_10[-1]))

	l_10_pct = 'NA'

	if denom !=0:
		l_10_pct = float(l_10[0])/denom


	return wins, losses, l_10_pct

if __name__ == '__main__':

	#print get_team_performance('mlb','New York Mets')
    wins, losses, l_10 = get_team_performance('mlb','New York Mets')
    print wins
    print losses
    print l_10

# Print onto csv file current date, team, wins, losses

# events = stubhub.event_list
#
# current_time = datetime.datetime.now()
# current_time_formatted = str(current_time.strftime("%Y-%m-%d %H:%M"))
#
# with open("csvs/records.csv", 'a') as csvfile:
# 	writer = csv.writer(csvfile)
#
# 	for event in events:
# 		sport = static_dictionaries.get_sport(event)
# 		team = static_dictionaries.get_team(event)
# 		full_team_name = static_dictionaries.get_full_team_name(team)
# 		sport = static_dictionaries.get_sport(event)
# 		wins = sports_wins[sport][full_team_name]
# 		losses = sports_losses[sport][full_team_name]
#  		l_10 = str(sports_l_10[sport][full_team_name]).replace("-"," ")
#
# 		writer.writerow([current_time_formatted,team, wins, losses, str(l_10)])
#
