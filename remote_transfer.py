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
        self.mysql_pass = ""

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
        for f in self.filenames:
            with open(f, newline='', encoding="utf8") as csvfile:
                spamreader = csv.reader(csvfile)
                for row in spamreader:
                    # Prepare SQL query to INSERT a record into the database.
                    sql = "INSERT INTO venue_linking_pairs(aid, mid) VALUES ('%s', '%s');" % (row[0], row[1])
                    print(sql)
                    try:
                        # Execute the SQL command
                        self.mycursor.execute(sql)
                    except:
                        # Rollback in case there is any error
                        print("There was an error.")
