import pandas as pd
import numpy as np
import os
from os import walk
import mysql.connector
from glom import glom


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

    def get_cursor(self):
        return self.mycursor

    def get_db():
        return self.mydb

    def disconnect_from_db(self):
        self.mycursor.close()
        self.mydb.close()
        print("Disconnected from DB")

    def get_filenames(self):
        mypath = "/home/ssarkar8/oag-project/oag_files"
        self.filenames = next(walk(mypath), (None, None, []))[2]  # [] if no file

    def add_to_sql(self):
        try:
            # Change filename here!
            df = pd.read_json("/home/ssarkar8/oag-project/oag_files/mag_venues.txt", lines=True, chunksize=1000000)
        except Exception as e:
            print(e)

        for chunk in df:
            count = 0
            print("Start creating inserts...")
            row = None
            try:
                for index, row in chunk.iterrows():
                    count += 1

                    # --- Uncomment for mag_papers ---
                    # authors_arr = row.authors
                    # auth_name_str = ""
                    # auth_id_str = ""
                    # for author in authors_arr:
                    #     auth_name_str += glom(author, 'name') + ','
                    #     auth_id_str += glom(author, 'id') + ','
                    # auth_name_str = auth_name_str.rstrip(',')
                    # auth_id_str = auth_id_str.rstrip(',')

                    # data = ('{}'.format(row.id), '{}'.format(row.title), '{}'.format(auth_name_str), '{}'.format(auth_id_str),
                    #         '{}'.format(glom(row.venue, 'id', default='')), '{}'.format(glom(row.venue, 'raw', default='')), int(row.year),
                    #         int(row.n_citation), '{}'.format(row.page_start), '{}'.format(row.page_end), '{}'.format(row.doc_type),
                    #         '{}'.format(row.publisher), '{}'.format(row.volume), '{}'.format(row.issue), '{}'.format(row.doi))
                    # self.mycursor.execute("INSERT INTO mag_papers (id, title, author_name, author_id, venue_id, venue_raw, year, n_citation, page_start, page_end, doc_type, publisher, volume, issue, doi) VALUES(%s, %s, %s, %s, NULLIF(%s,''), NULLIF(%s,''),%s, %s,%s,NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''), NULLIF(%s,''))", data)
                    # self.mydb.commit()

                    # --- Uncomment for aminer_papers ---
                    # authors_arr = row.authors
                    # auth_name_str = ""
                    # auth_id_str = ""
                    # for author in authors_arr:
                    #     auth_name_str += glom(author, 'name') + ','
                    #     auth_id_str += glom(author, 'id') + ','
                    # auth_name_str = auth_name_str.rstrip(',')
                    # auth_id_str = auth_id_str.rstrip(',')

                    # data = ('{}'.format(row.id), '{}'.format(row.title), '{}'.format(auth_name_str), '{}'.format(auth_id_str),
                    #         '{}'.format(glom(row.venue, 'id', default='')), '{}'.format(glom(row.venue, 'raw', default='')), int(row.year),
                    #         int(row.n_citation), '{}'.format(row.page_start), '{}'.format(row.page_end), '{}'.format(row.doc_type),
                    #         '{}'.format(row.publisher), '{}'.format(row.volume), '{}'.format(row.issue), '{}'.format(row.doi))
                    # self.mycursor.execute("INSERT INTO aminer_papers (id, title, author_name, author_id, venue_id, venue_raw, year, n_citation, page_start, page_end, doc_type, publisher, volume, issue, doi) VALUES(%s, %s, %s, %s, NULLIF(%s,''), NULLIF(%s,''),%s, %s,%s,NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''), NULLIF(%s,''))", data)
                    # self.mydb.commit()

                    # --- Uncomment for mag_authors/aminer_authors ---
                    # pubs_arr = row.pubs
                    # pubs_i_str = ""
                    # pubs_r_str = ""
                    # for pub in pubs_arr:
                    #     pubs_i_str += str(glom(pub, 'i')) + ','
                    #     pubs_r_str += str(glom(pub, 'r')) + ','
                    # pubs_i_str = pubs_i_str.rstrip(',')
                    # pubs_r_str = pubs_r_str.rstrip(',')

                    # tags_arr = row.tags
                    # tags_str = ""
                    # if type(tags_arr) != float:
                    #     for tag in tags_arr:
                    #         tags_str += str(glom(tag, 't')) + ','
                    #     tags_str = tags_str.rstrip(',')

                    # # id, name, normalized_name, pubs_i, pubs_r, n_pubs, n_citation, tags
                    # data = ('{}'.format(row.id), '{}'.format(row.name), '{}'.format(row.normalized_name), '{}'.format(pubs_i_str),
                    #         '{}'.format(pubs_r_str), int(row.n_pubs), int(row.n_citation), '{}'.format(tags_str))
                    # debug_str = "%s, %s, %s, %s, %s, %d, %d, %s" % data
                    # self.mycursor.execute("INSERT INTO mag_authors (id, name, normalized_name, pubs_i, pubs_r, n_pubs, n_citation, tags_t) VALUES(%s, %s, %s, %s, NULLIF(%s,''), NULLIF(%s,''),%s, NULLIF(%s,''))", data)
                    # self.mydb.commit()

                    # --- Uncomment for mag_venues/aminer_venues ---
                    # data = ('{}'.format(row.id), '{}'.format(row.DisplayName), '{}'.format(row.NormalizedName), '{}'.format(row.JournalId))
                    # debug_str = "%s, %s, %s, %s" % data
                    # self.mycursor.execute("INSERT INTO mag_venues (id, DisplayName, NormalizedName, JournalId) VALUES(%s, %s, %s, %s)", data)
                    # self.mydb.commit()

                    if count % 100000 == 0:
                        print("Inserted 100,000 more rows")
            except Exception as e:
                print(e)
                print(debug_str)
                self.disconnect_from_db()
                break
            print("Inserted {} rows".format(count))

