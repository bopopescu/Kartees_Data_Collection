#from lxml import etree
import urllib
from bs4 import BeautifulSoup
import csv
import scripts.stubhub
import scripts.static_dictionaries
import datetime
import pdb
import time


def get_team_performance(sport, team):

	sport = sport.lower()
	first = time.time()
	current_sport = sport
	full_team_name = team if 'Clippers' not in team else 'LA Clippers'

	sports = ['nba','mlb','nhl','nfl']
	#sports = ['mlb']
	stats_row_buffer = {'mlb': 13, 'nba': 14, 'nhl': 16, 'nfl': 13}

	sports_wins = {}
	sports_losses = {}
	sports_l_10 = {}

	if sport.lower() !='nhl':

		second = time.time()

		web_page = urllib.urlopen("http://espn.go.com/%s/standings" % sport.lower()).read()
		
		third = time.time()
		soup = BeautifulSoup(web_page, "html.parser")

		#teams = soup.find_all("span", class_= "team-names")
		rows = soup.find_all(class_="standings-row")

		#pdb.set_trace()
		wins = {}
		losses = {}
		l_10 = {}

		#i=0
		for row in rows:
			team_name = row.find(class_="team").find(class_="team-names").text
			wins[team_name] = row.find_all("td")[1].text
			losses[team_name] = row.find_all("td")[2].text

			if sport.lower() =='nba':
				l_10[team_name] = row.find_all("td")[13].text
			elif sport.lower() == 'mlb':
				l_10[team_name] = row.find_all("td")[11].text
				

		 	#k += stats_row_buffer[sport]


		sports_wins[sport] = wins
		sports_losses[sport] = losses
		sports_l_10[sport] = l_10

		

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

		

	#print sports_wins
	
	wins = sports_wins[current_sport][full_team_name]
	losses = sports_losses[current_sport][full_team_name]
	l_10 = str(sports_l_10[current_sport][full_team_name]).replace("-"," ")[0:3]

	denom = (float(l_10[0])+ float(l_10[-1]))

	l_10_pct = 'NA'
	
	if denom !=0:
		l_10_pct = float("%.2f" %(float((l_10[0]))/denom))


	return wins, losses, l_10_pct

if __name__ == '__main__':

	#print get_team_performance('mlb','New York Mets')
    wins, losses, l_10 = get_team_performance('mlb','New York Mets')
    # print wins
    # print losses
    # print l_10

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
