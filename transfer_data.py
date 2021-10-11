import mysql.connector
from mysql.connector import errorcode
import paramiko
import pandas as pd

class TransferData:

    # ssh info
    ssh_host = "" # hostname
    ssh_user = "" # username (netid)
    ssh_password = ""
    ssh_client = paramiko.SSHClient()
    # db info
    mysql_host = "" # sql hostname (owl2.cs.illinois.edu)
    mysql_user = "" # sql username (netid)
    mysql_pass = ""
    # mysql connector items
    mydb = "" # connection
    mycursor = "" # cursor

    def __init__(self, user, password):
        self.ssh_host = "falcon.cs.illinois.edu"
        self.ssh_user = user
        self.ssh_password = password
        self.mysql_host = "owl2.cs.illinois.edu"
        self.mysql_user = "ssarkar"
        self.mysql_pass = "Ab33sE@D"

    def connect_to_SSH(self):
        # print("Connecting...")
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(hostname=self.ssh_host,
                           username=self.ssh_user,
                           password=self.ssh_password)
        print("Connected")

    def disconnect_from_SSH(self):
        self.ssh_client.close()
        print("Disconnected from SSH")

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

    TABLES = {}
    def create_tables_dict(self):
        TABLES = self.TABLES
        # venue_linking_pairs
        TABLES['venue_linking_pairs'] = (
        "CREATE TABLE `venue_linking_pairs` ("
        "  `mid` varchar(24) NOT NULL,"
        "  `aid` varchar(24) NOT NULL"
        ")" )
        # aminer_venues
        TABLES['aminer_venues'] = (
            "CREATE TABLE `aminer_venues` ("
            "  `id` varchar(24) NOT NULL,"
            "  `JournalId` varchar(20),"
            "  `ConferenceId` varchar(20),"
            "  `DisplayName` varchar(100),"
            "  `NormalizedName` varchar(100)"
            ")")
        # mag_venues
        TABLES['mag_venues'] = (
            "CREATE TABLE `mag_venues` ("
            "  `id` varchar(24) NOT NULL,"
            "  `JournalId` varchar(20),"
            "  `ConferenceId` varchar(20),"
            "  `DisplayName` varchar(100),"
            "  `NormalizedName` varchar(100)"
            ")")
        # paper_linking_pairs
        TABLES['paper_linking_pairs'] = (
            "CREATE TABLE `paper_linking_pairs` ("
            "  `mid` varchar(24) NOT NULL,"
            "  `aid` varchar(24) NOT NULL"
            ")" )
        # aminer_papers
        TABLES['aminer_papers'] = (
            "CREATE TABLE `aminer_papers` ("
            "  `id` varchar(24) NOT NULL,"
            "  `title` varchar(150),"
            "  `author.name` varchar(50),"
            "  `author.org` varchar(100),"
            "  `author.id` varchar(24),"
            "  `venue.id` varchar(24),"
            "  `venue.raw` varchar(100),"
            "  `year` INT,"
            "  `keywords` varchar(200),"
            "  `n_citation` INT,"
            "  `page_start` varchar(5),"
            "  `page_end` varchar(5),"
            "  `doc_type` varchar(20),"
            "  `lang` varchar(5),"
            "  `publisher` varchar(20),"
            "  `volume` varchar(4),"
            "  `issue` varchar(4),"
            "  `issn` varchar(12),"
            "  `isbn` varchar(13),"
            "  `doi` varchar(128),"
            "  `pdf` varchar(150),"
            "  `url` varchar(300),"
            "  `abstract` varchar(500)"
            ")")
        # mag_papers
        TABLES['mag_papers'] = (
            "CREATE TABLE `mag_papers` ("
            "  `id` varchar(24) NOT NULL,"
            "  `title` varchar(150),"
            "  `author.name` varchar(50),"
            "  `author.org` varchar(100),"
            "  `author.id` varchar(24),"
            "  `venue.id` varchar(24),"
            "  `venue.raw` varchar(100),"
            "  `year` INT,"
            "  `keywords` varchar(200),"
            "  `n_citation` INT,"
            "  `page_start` varchar(5),"
            "  `page_end` varchar(5),"
            "  `doc_type` varchar(20),"
            "  `lang` varchar(5),"
            "  `publisher` varchar(20),"
            "  `volume` varchar(4),"
            "  `issue` varchar(4),"
            "  `issn` varchar(12),"
            "  `isbn` varchar(13),"
            "  `doi` varchar(128),"
            "  `pdf` varchar(150),"
            "  `url` varchar(300),"
            "  `abstract` varchar(500)"
            ")")
        # author_linking_pairs
        TABLES['author_linking_pairs'] = (
            "CREATE TABLE `author_linking_pairs` ("
            "  `mid` varchar(24) NOT NULL,"
            "  `aid` varchar(24) NOT NULL"
            ")")
        # aminer_authors
        TABLES['aminer_authors'] = (
            "CREATE TABLE `aminer_authors` ("
            "  `id` varchar(24) NOT NULL,"
            "  `name` varchar(60),"
            "  `normalized_name` varchar(60),"
            "  `orgs` varchar(200),"
            "  `org` varchar(100),"
            "  `position` varchar(30),"
            "  `n_pubs` INT,"
            "  `n_citation` INT,"
            "  `h_index` INT,"
            "  `tags.t` varchar(100),"
            "  `tags.w` INT,"
            "  `pubs.i` varchar(25),"
            "  `pubs.r` INT"
            ")")
        # mag_authors
        TABLES['mag_authors'] = (
            "CREATE TABLE `mag_authors` ("
            "  `id` varchar(24) NOT NULL,"
            "  `name` varchar(60),"
            "  `normalized_name` varchar(60),"
            "  `orgs` varchar(200),"
            "  `org` varchar(100),"
            "  `position` varchar(30),"
            "  `n_pubs` INT,"
            "  `n_citation` INT,"
            "  `h_index` INT,"
            "  `tags.t` varchar(100),"
            "  `tags.w` INT,"
            "  `pubs.i` varchar(25),"
            "  `pubs.r` INT"
            ")")

    def create_tables(self):
        TABLES = self.TABLES
        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                print("Creating table {}: ".format(table_name), end='')
                self.mycursor.execute(table_description)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            else:
                print("OK")

    def show_tables(self):
        self.mycursor.execute("SHOW TABLES")

        for table_name in self.mycursor:
            print(table_name)

    def unzip_files(self):
        ssh = self.ssh_client
        ssh.exec_command("cd /home/ssarkar8/oag-project/oag_files")
        ssh.exec_command("unzip \*.zip")
        print("Zip files extracted")
        ssh.exec_command("rm -f *.zip")
