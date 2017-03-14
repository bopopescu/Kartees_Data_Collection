# Constants
SANDBOX_URL = 'https://api.stubhubsandbox.com'
PROD_URL = 'https://api.stubhub.com'

USERNAME = 'abelabo30@aol.com'
PASSWORD = 'nyjets'
BASIC_AUTH_SAND = 'ZjRjSlptUGZ0VExSQklLdDdtaHJiYU5pOW9ZYTpCZnVfbDVyTzZUSklscU9Cbl9MSlpJeXk5Sklh'
APP_TOKEN_SAND = '6u6p394ZmCk75NfNPUZ0gHR74dMa'
BASIC_AUTH_PROD = 'TlczU0ZMbVgycXNEenp1S3IzOGdkdExHbnlrYTpBVHhib3pJSjhsS3A5TnJQcVROdXZraGloREVh'
APP_TOKEN_PROD = 'He3oVj6yqxvkHfSB8Rd7rAL0tvAa'
CLIENT_ID = 'NW3SFLmX2qsDzzuKr38gdtLGnyka'
CLIENT_SECRET = 'ATxbozIJ8lKp9NrPqTNuvkhihDEa'


USERNAME_MO = 'mgzeitouni@gmail.com'
PASSWORD_MO = 'morris12'
BASIC_AUTH_PROD_MO = 'QWNSQUNfZjdXenYzQjRZaG1wakpUazFPYjBjYTplSzVxSll0bVYwb2diRDdRRHFtMGhIUENfQ2th'
APP_TOKEN_PROD_MO = 'sllPgZnDi98izhNL63NMp5oTTTka'
CLIENT_ID_MO = 'AcRAC_f7Wzv3B4YhmpjJTk1Ob0ca'
CLIENT_SECRET_MO = 'eK5qJYtmV0ogbD7QDqm0hHPC_Cka'

account = 'mo'

BASIC_AUTH = BASIC_AUTH_PROD
APP_TOKEN = APP_TOKEN_PROD

if (account=='mo'):
    BASIC_AUTH = BASIC_AUTH_PROD_MO
    APP_TOKEN = APP_TOKEN_PROD_MO
    USERNAME = USERNAME_MO
    PASSWORD = PASSWORD_MO
    CLIENT_ID = CLIENT_ID_MO
    CLIENT_SECRET = CLIENT_SECRET_MO

#Specify keys to use - Sandbox or Prod
URL = PROD_URL

CREDS = {
			'MO': {
				'BASIC_AUTH':'QWNSQUNfZjdXenYzQjRZaG1wakpUazFPYjBjYTplSzVxSll0bVYwb2diRDdRRHFtMGhIUENfQ2th',
				'APP_TOKEN':'sllPgZnDi98izhNL63NMp5oTTTka',
				'CLIENT_ID':'AcRAC_f7Wzv3B4YhmpjJTk1Ob0ca',
				'CLIENT_SECRET':'eK5qJYtmV0ogbD7QDqm0hHPC_Cka',
				'USERNAME':'mgzeitouni@gmail.com',
				'PASSWORD':'morris12'
				},

			'LABO':{
				'BASIC_AUTH':'TlczU0ZMbVgycXNEenp1S3IzOGdkdExHbnlrYTpBVHhib3pJSjhsS3A5TnJQcVROdXZraGloREVh',
				'APP_TOKEN':'He3oVj6yqxvkHfSB8Rd7rAL0tvAa',
				'CLIENT_ID':'NW3SFLmX2qsDzzuKr38gdtLGnyka',
				'CLIENT_SECRET':'ATxbozIJ8lKp9NrPqTNuvkhihDEa',
				'USERNAME':'abelabo30@aol.com',
				'PASSWORD':'nyjets'
				}

		}