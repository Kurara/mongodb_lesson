from pymongo import MongoClient


class MongoDBManagement:

    """ Global functions
    """
    def connect_db(self, database=None):
        client = MongoClient('mongodb://root:password@localhost:27017/')
        self.cnx = client[database]
    
    def insert(self, table, data):
        inserted = self.cnx[table].insert_one(data)

        return inserted.inserted_id

    def _scape_string(self, input):
        new_value = str(input).replace("'", "\\'")
        return "'" + new_value + "'"

    def disconect_db(self):
        self.cnx.close()
