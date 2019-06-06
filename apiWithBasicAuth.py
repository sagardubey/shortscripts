# This code contains two functions to access any login API that supports authentication
# via Basic Auth (passing username password in Body)
# It also converts the UUID token to a Base64 encoded token that can be passed as an 
# Authorization value for subsequent API transactions

import requests
import json
import base64

# Define base URL and paths
baseUrl = 'https://example.com'
tokensPath = '/v1/tokens'

# Generate token from username password
def getToken(username,password):
	userpass = {}
	userpass['userName'] = username
	userpass['password'] = password
	tokensEndpoint = baseUrl + tokensPath
	r = requests.post(tokensEndpoint, json=userpass)
	b = json.loads(r.text)
	return b['token']

# Encode token string to base64
def base64encode(tokenString):
        print(type(tokenString))
        print(tokenString)
        #print(tokenString['token'])
        byteToken = tokenString.encode('utf-8')
        encodedToken = base64.b64encode(byteToken)
        s = encodedToken.decode('utf-8')
        finalToken = 'Basic ' + s
        return finalToken


tokenString = getToken('sagardubey','8050321056-sagardubey')

base64encode(tokenString)



