import pandas as pd
import os
from os import walk
import csv
import mysql.connector

class RemoteTransfer:
    filenames = []
    # db info
    mysql_host = ""  # sql hostname (owl2.cs.illinois.edu)
    mysql_user = ""  # sql username (netid)
    mysql_pass = ""
    # mysql connector items
    mydb = ""  # connection
    mycursor = ""  # cursor

    def __init__(self):
        self.mysql_host = "owl2.cs.illinois.edu"
        self.mysql_user = "ssarkar"
        self.mysql_pass = "Ab33sE@D"

    def connect_to_database(self):
        DB_NAME = 'ssarkar_oag'
        # print("Connecting to DB...")
        self.mydb = mysql.connector.connect(user=self.mysql_user,
                                            password=self.mysql_pass,
                                            host=self.mysql_host)
        print("Connected to DB")
        self.mycursor = self.mydb.cursor()

        # connect to ssarkar_oag database
        try:
            self.mycursor.execute("USE {}".format(DB_NAME))
            self.mydb.commit()
            print("Connected to", DB_NAME)
        except mysql.connector.Error as err:
            print(err)
            exit(1)

    def disconnect_from_db(self):
        self.mycursor.close()
        self.mydb.close()
        print("Disconnected from DB")

    def get_filenames(self):
        mypath = "/home/ssarkar8/oag-project/oag_files"
        self.filenames = next(walk(mypath), (None, None, []))[2]  # [] if no file

    def add_to_sql(self):
        df = pd.read_json("/home/ssarkar8/oag-project/oag_files/author_linking_pairs.txt", lines=True)
        count = 0
        try:
            for index, row in df.iterrows():
                count += 1
                data = ('{}'.format(row.mid), '{}'.format(row.aid))
                self.mycursor.execute("INSERT INTO author_linking_pairs (mid,aid) values(%s,%s)", data)
                self.mydb.commit()
                if count % 10000 == 0:
                    print("Inserted 10,000 more rows")
        except:
            print("error")
        print("Inserted {} rows".format(count))
