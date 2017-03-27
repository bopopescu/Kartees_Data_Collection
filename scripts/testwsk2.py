from stubhub import *

def main(dict):

	stubhub = Stubhub(account = 'LABO')
	event = stubhub.get_event(9710889)
	return {"response": event.status_code}

if __name__ == '__main__':

	print main('he')