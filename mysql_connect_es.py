import conf
from elasticsearch import Elasticsearch
from datetime import datetime
import os
import mysql.connector
from remote_transfer import RemoteTransfer


class ConnectToES:
    def __init__(self):
        _es = None
        self.index_name = conf.INDEX_NAME
        self.index_type = conf.INDEX_TYPE

    ''' 
        This method establishes a connection to the ElasticSearch server
        through port 9200
    '''

    def connect_elasticsearch(self):
        self._es = Elasticsearch("http://localhost:9200")
        if self._es.ping():
            print('Yay Connect')
        else:
            print('Awww it could not connect!')
        return self._es

    ''' 
        This method is for testing/debugging purposes. It allows one to check
        whether the ElasticSearch server connection is working properly. Also
        checks whether one is able to create an index, add a document, and 
        retrieve a document.
    '''

    def test_connect(self):
        es = self._es
        document = {
            "description": "this is a test",
            "timestamp": datetime.now()
        }
        index = "testing"
        doc_id = 1
        es.index(index=index, doc_type="test", id=doc_id, body=document)

        print("Indexed document to index \"" + index + "\" with id " + str(doc_id))
        result = es.get(index=index, doc_type="test", id=doc_id)
        retrieved_document = result['_source']
        print("Retrieved document: {Id: " + result['_id'] + ", Description: " +
              retrieved_document['description'] + ", Timestamp: " +
              retrieved_document['timestamp'] + "}")

    '''
        This method disconnects from the ElasticSearch server.
    '''

    def disconnect_es(self):
        self._es.transport.connection_pool.close()
        if not self._es.ping():
            print('Disconnection Successful')
        else:
            print('Failed to disconnect')

    def create_index(self):
        # Create index mapping.
        _index_mappings = {
            # Index settings.
            "settings": {
                "index": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            },
            # configure index fields.
            "mappings": {
                "author_linking_pairs": {
                    "properties": {
                        "mid": {"type": "text"},
                        "aid": {"type": "text"},
                    }
                },
                "venue_linking_pairs": {
                    "properties": {
                        "mid": {"type": "text"},
                        "aid": {"type": "text"},
                    }
                },
                "paper_linking_pairs": {
                    "properties": {
                        "mid": {"type": "text"},
                        "aid": {"type": "text"},
                    }
                },
                "aminer_authors": {
                    "properties": {
                        "id": {"type": "text"},
                        "name": {"type": "text"},
                        "h_index": {"type": "integer"},
                        "n_pubs": {"type": "integer"},
                        "tags": {
                            "type": "nested",
                            "properties": {
                                "w": {"type": "integer"},
                                "t": {"type": "text"}
                            }
                        },
                        "n_citation": {"type": "integer"},
                        "pubs": {
                            "type": "nested",
                            "properties": {
                                "i": {"type": "text"},
                                "r": {"type": "integer"}
                            }
                        },
                        "orgs": {"type": "text"}
                    }
                },
                "aminer_papers": {
                    "properties": {
                        "id": {"type": "text"},
                        "title": {"type": "text"},
                        "authors": {
                            "type": "nested",
                            "properties": {
                                "name": {"type": "text"},
                                "id": {"type": "text"}
                            }
                        },
                        "venue": {
                            "type": "nested",
                            "properties": {
                                "raw": {"type": "text"},
                                "id": {"type": "text"}
                            }
                        },
                        "year": {"type": "integer"},
                        "n_citation": {"type": "integer"},
                        "page_start": {"type": "text"},
                        "page_end": {"type": "text"},
                        "lang": {"type": "text"},
                        "volume": {"type": "text"},
                        "issue": {"type": "text"},
                        "doi": {"type": "text"},
                        "url": {"type": "text"}
                    }
                },
                "mag_authors": {
                    "properties": {
                        "id": {"type": "text"},
                        "name": {"type": "text"},
                        "h_index": {"type": "integer"},
                        "n_pubs": {"type": "integer"},
                        "tags": {
                            "type": "nested",
                            "properties": {
                                "w": {"type": "integer"},
                                "t": {"type": "text"}
                            }
                        },
                        "n_citation": {"type": "integer"},
                        "pubs": {
                            "type": "nested",
                            "properties": {
                                "i": {"type": "text"},
                                "r": {"type": "integer"}
                            }
                        },
                        "orgs": {"type": "text"}
                    }
                },
                "mag_papers": {
                    "properties": {
                        "id": {"type": "text"},
                        "title": {"type": "text"},
                        "authors": {
                            "type": "nested",
                            "properties": {
                                "name": {"type": "text"},
                                "id": {"type": "text"}
                            }
                        },
                        "venue": {
                            "type": "nested",
                            "properties": {
                                "raw": {"type": "text"},
                                "id": {"type": "text"}
                            }
                        },
                        "year": {"type": "integer"},
                        "n_citation": {"type": "integer"},
                        "page_start": {"type": "text"},
                        "page_end": {"type": "text"},
                        "lang": {"type": "text"},
                        "volume": {"type": "text"},
                        "issue": {"type": "text"},
                        "doi": {"type": "text"},
                        "url": {"type": "text"}
                    }
                }
            }
        }
        # If there is not any index existing in the elasticsearch server.
        if self._es.indices.exists(index=self.index_name) is not True:
            # Then create the index in elasticsearch server.
            res = self._es.indices.create(index=self.index_name, body=_index_mappings)
            # print(res)
            if not res:
                print("create index failed")
                return False
            print("create index success")
            return True
        else:
            print("index already exists")
            return True

    def bulk_insert(self, table, data_list):
        # Create a temporary data list.
        actions = []
        i = 0

        try:
            # Create the temporary data list ( which will be inserted into elasticsearch server ) by loop the input data list.
            for data in data_list:
                # Create one JSON format document that will be inserted into elasticsearch server.
                action = {
                    "_index": self.index_name,
                    "_type": self.index_type,
                    "_id": i,
                    # _id ( It can also be generated by elasticsearch server automatically without assignment. )
                    "_source": {
                        'mid': data[0],
                        'aid': data[1]
                    }
                }

                # Add above JSON format elasticsearch document to data list.
                actions.append(action)
                # When the number of list elements reaches MAX.
                if len(action) == conf.MAXIMUM:
                    # Batch insert the data into elasticsearch server.
                    helpers.bulk(self._es, actions)
                    # Empty the temporary data list.
                    del actions[0:len(action)]

            # When there are less than MAX elements in the input data list then insert the remaining data into elasticsearch server,
            if i > 0:
                helpers.bulk(self._es, actions)
            i += 1
            print("Done")
            return True
        except Exception as e:
            print(e)
            return False

    def select(self, sql):
        try:
            remote = RemoteTransfer()
            cxn = remote.connect_to_database()
            # Create the database cursor.
            cur = remote.get_cursor()
            # Get database
            # mydb = remote.get_db()
            # Execute the select SQL statement.
            cur.execute(sql)
            # Get above sql execution results/
            res = cur.fetchall()
            # Close the connection to the MySQL server.
            remote.disconnect_from_db()
            # Return the select results/
            return res
        except Exception as e:
            print(e)
            return False

    def read_mysql_es(self):
        # Create elasticsearch data index.
        if self.create_index() is False:
            # print(2)
            return False
        # Maximum row number of one query at one time.
        max = conf.MAXIMUM

        # The flag_list is used to records unexist table.
        flag_list = []
        mysql_host = "owl2.cs.illinois.edu"
        mysql_user = "ssarkar"
        mysql_pass = "Ab33sE@D"
        mydb = mysql.connector.connect(user=mysql_user,
                                       password=mysql_pass,
                                       host=mysql_host)

        # Transfer tables records to elasticsearch server.
        for i in range(1):
            # id = 0
            while True:
                # Query records from the table.
                sql = "select * from author_linking_pairs"
                data_list = self.select(sql)
                # Break the loop if the select result is empty.
                if not data_list:
                    print("The sql: execution return empty results, no records to write to elasticsearch server.")
                    break
                    # The last row in the table.
                last_row = data_list[-1]
                # Modify the maximum id value.
                # id = last_row['id']
                # Batch insert the MySQL table row data into elasticsearch server.
                res = self.bulk_insert('author_linking_pairs', data_list)
                if not res:
                    print("Transfer table rows to elasticsearch server fail.")
                    return False

        print("table's rows has been transfered to elasticsearch server successfully")

    def show_indexes(self):
        print(self._es.indices.get_alias("*"))

    '''
    Search queries starting from here:
    '''

    def search(es_object, index_name, search):
        res = es_object.search(index=index_name, body=search)

    def search_author_by_keyword(self, keyword, print_tags=False):
        try:
            remote = RemoteTransfer()
            cxn = remote.connect_to_database()
            # Create the database cursor.
            cur = remote.get_cursor()
            # Execute the select SQL statement.
            sql = "SELECT normalized_name, tags_t FROM mag_authors WHERE LOWER(tags_t) LIKE LOWER('%{}%');".format(
                keyword)
            cur.execute(sql)

            for name, tags_t in cur:
                if print_tags:
                    print("Author: ", name, " Tags: ", tags_t)
                else:
                    print("Author: ", name)

            print("Successfully printed authors with keyword {}".format(keyword))
            # Close the connection to the MySQL server.
            remote.disconnect_from_db()
        except Exception as e:
            print(e)
            return False

    def search_author_name(self, name):
        try:
            remote = RemoteTransfer()
            cxn = remote.connect_to_database()
            # Create the database cursor.
            cur = remote.get_cursor()
            # Execute the select SQL statement.
            sql = "SELECT normalized_name FROM mag_authors WHERE LOWER(author_name) LIKE LOWER('%{}%');".format(name)
            cur.execute(sql)

            for author in cur:
                if print_author:
                    print(" Author: ", author)

            print("Successfully printed authors named {}".format(name))
            # Close the connection to the MySQL server.
            remote.disconnect_from_db()
        except Exception as e:
            print(e)
            return False

    def search_paper_by_author_name(self, name, print_author=False):
        try:
            remote = RemoteTransfer()
            cxn = remote.connect_to_database()
            # Create the database cursor.
            cur = remote.get_cursor()
            # Execute the select SQL statement.
            sql = "SELECT title, author_name FROM mag_papers WHERE LOWER(author_name) LIKE LOWER('%{}%');".format(name)
            cur.execute(sql)

            for title, author in cur:
                if print_author:
                    print("Title: ", title, " Author: ", author)

            print("Successfully printed papers written by {}".format(name))
            # Close the connection to the MySQL server.
            remote.disconnect_from_db()
        except Exception as e:
            print(e)
            return False

    def search_paper_by_keyword(self, keyword, print_author=False):
        try:
            remote = RemoteTransfer()
            cxn = remote.connect_to_database()
            # Create the database cursor.
            cur = remote.get_cursor()
            # Execute the select SQL statement.
            sql = "SELECT p.title FROM (SELECT a.normalized_name, a.tags_t, p.title FROM mag_authors a WHERE LOWER(tags_t) LIKE LOWER('%cancer%') AS b JOIN mag_papers p ON b.id = p.author_id) AS auth;".format(
                keyword)
            cur.execute(sql)

            for title, author in cur:
                if print_author:
                    print("Title: ", title, " Author: ", author)

            print("Successfully printed papers with keyword {}".format(keyword))
            # Close the connection to the MySQL server.
            remote.disconnect_from_db()
        except Exception as e:
            print(e)
            return False

    def search_paper_by_doi(self, doi):
        try:
            remote = RemoteTransfer()
            cxn = remote.connect_to_database()
            # Create the database cursor.
            cur = remote.get_cursor()
            # Execute the select SQL statement.
            sql = "SELECT title FROM mag_papers WHERE doi LIKE '%{}%';".format(doi)
            cur.execute(sql)

            for title in cur:
                print("Title: ", title)

            print("Successfully printed papers with doi {}".format(doi))
            # Close the connection to the MySQL server.
            remote.disconnect_from_db()
        except Exception as e:
            print(e)
            return False