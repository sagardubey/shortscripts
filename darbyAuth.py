# The functions defined here let you access Darby APIs

import requests
import json
import base64

# Define base URL and paths
tokensEndpoint = 'https://al-apigateway.drivewithdarby.com/v1/tokens'

# Generate token from username password
def getDarbyToken(username,password):
	userpass = {}
	userpass['userName'] = username # Taking username pwd input
	userpass['password'] = password
	r = requests.post(tokensEndpoint, json=userpass) # posting to API
	b = json.loads(r.text) # Storing the response text
	bToken = b['token']
	byteToken = bToken.encode('utf-8') # Converting to bytes
	encodedToken = base64.b64encode(byteToken)
	s = encodedToken.decode('utf-8') # Converting back to string
	decodedToken = 'Basic ' + s # Forming the token string
	return decodedToken
