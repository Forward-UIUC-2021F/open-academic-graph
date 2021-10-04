class TransferData:
    import mysql.connector

    host = ""
    user = ""
    password = ""

    def __init__(self, user, password):
        self.host = "ssarkar8@falcon.cs.illinois.edu"
        self.user = user
        self.password = password

    def connect_to_database(self):
        mydb = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password
        )

        mycursor = mydb.cursor()

        mycursor.execute("SHOW DATABASES")

        for x in mycursor:
            print(x)
