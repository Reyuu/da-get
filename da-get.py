import requests
import mysql.connector
import time
from configparser import ConfigParser
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError

#TODO: Rewrite for MySQL

class Api:
    def __init__(self, auth_url, base_url, config, headers):
        self.auth_url = auth_url
        self.base_url = base_url
        self.headers = headers
        self.client_id = config["DeviantArtAPI"]["ID"]
        self.client_secret = config["DeviantArtAPI"]["Secret"]
        client1 = BackendApplicationClient(client_id=self.client_id)
        self.oauth = OAuth2Session(client=client1)
        self.client = BackendApplicationClient(client_id=self.client_id, client_secret=self.client_secret)
        self.da = OAuth2Session(client=self.client, token=self.get_token())

    def refresh_session(self):
        self.da = OAuth2Session(client=self.client, token=self.get_token())

    def get_token(self):
        return self.oauth.fetch_token(token_url='%stoken' % self.auth_url, client_id=self.client_id, client_secret=self.client_secret)

    def get_resource(self, endpoint, params):
        try:
            url = "%s%s" % (self.base_url, endpoint)
            r = self.da.get(url, params=params, headers=self.headers)
            return r
        except TokenExpiredError as e:
            self.refresh_session()
            return self.get_resource(endpoint, params)

class Parser:
    def __init__(self, cursor, api_class):
        self.db = cursor
        self.api_class = api_class
        self.last_published_time = 0
    
    def parse_gallery(self, username, offset):
        self._parse_gallery(username, offset)
        self.db.execute("SELECT * FROM mainapp_donedevs WHERE username = %s", (username,))
        record = self.db.fetchone()
        try:
            print("record: %s" % record)
        except TypeError:
            print("record: %s" % ",".join(map(str, record)))
        if record == None:
            self.db.execute("INSERT INTO mainapp_donedevs (username, last_updated, last_published_date) VALUES (%s, %s, %s)", (username, int(time.time()), self.last_published_time))
        else:
            self.db.execute("UPDATE mainapp_donedevs SET last_updated = %s, last_published_date = %s WHERE username = %s", (int(time.time()), self.last_published_time, username))
        #self.db.close()

    def _parse_gallery(self, username, offset):
        timeout = 1
        while True:
            r = self.api_class.get_resource("gallery/all", params={"username": username, "offset": offset, "limit": "24", "mature_content": "true"})
            if r.status_code == 404:
                print("404'd")
                return False
            if r.status_code >= 400:
                time.sleep(timeout)
                print("TIMEOUT %s: %s" % (r.status_code, timeout))
                #exponential timeout as recommended in docs
                timeout = timeout*2
            else:
                break
        json = r.json()
        #print(json)
        #check if there's more
        has_more = json["has_more"]
        next_offset = json["next_offset"]
        #parse results
        for i in json["results"]:
            id_ = i["deviationid"]
            p_time = i["published_time"]
            self.last_published_time = p_time
            self.db.execute("SELECT * FROM mainapp_devs WHERE published_time = %s and deviationid = %s", (p_time, id_))
            if not(len(self.db.fetchall()) == 0):
                continue
            is_mature = 1 if i["is_mature"] == True else 0
            try:
                values = (id_, i["preview"]["src"], i["url"], p_time, is_mature, 1)
                self.db.execute("INSERT INTO mainapp_devs (deviationid, content_src, url, published_time, is_mature, viewable) values (%s, %s, %s, %s, %s, %s)", values)
            except KeyError:
                print("ERROR at: %s" % i)
        #finally
        if has_more:
            self._parse_gallery(username, next_offset)


if __name__ == "__main__":
    auth_url = "https://www.deviantart.com/oauth2/"
    base_url = "https://www.deviantart.com/api/v1/oauth2/"
    headers = {"user-agent": "my-app/1-20180714", "Accept-Encoding": "gzip, deflate"}
    config = ConfigParser()
    config.read("config.ini")

    api = Api(auth_url, base_url, config, headers)
    #r = api.get_resource("gallery/all", params={"username": "senshistock", "offset": "0", "limit": "24"})
    #print(r.text)
    db_config = config["MySQLDatabase"]
    conn = mysql.connector.connect(user=db_config["Username"], password=db_config["Password"], host=db_config["Host"], database=db_config["Database"])
    cursor = conn.cursor()
    parser = Parser(cursor, api)
    stocks = config["Misc"]["Stocks"].split(",")
    
    for stock in stocks:
        try:
            parser.parse_gallery(stock, 0)
        except Exception as e:
            print("ERR: Stock: %s, %s" % (stock, str(e)))
   


    cursor.close()
    conn.commit()
    conn.close()