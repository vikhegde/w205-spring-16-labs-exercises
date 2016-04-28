import tweepy

consumer_key = "a6lYwG2Lb8LEwmPtyCORxTDKJ";
#eg: consumer_key = "YisfFjiodKtojtUvW4MSEcPm";


consumer_secret = "kDuWxQyE58WiM86Qd7K6vZVqG9FHn7pyCUjJydTyylcejI3qxO";
#eg: consumer_secret = "YisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPm";

access_token = "35762850-hdyQH3uf3oq2RtfLVDPeq6qpDHDFXuGU8FgZE7f4R";
#eg: access_token = "YisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPm";

access_token_secret = "FlSQI0wKYYP1i2TS6KEXFP52GoDUmhKpM6tIwbz10mYqh";
#eg: access_token_secret = "YisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPm";


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)



