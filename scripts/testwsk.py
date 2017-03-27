import urllib2

def main(dict):

	call = urllib2.urlopen('https://stubhub-services-node-red-dev.mybluemix.net/testing').read()
	response = {"response": call}
	print response
	return response