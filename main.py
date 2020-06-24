from pymongo import MongoClient


class MongoDBManagement:

    """ Global functions
    """
    def connect_db(self, collection=None):
        client = MongoClient('mongodb://root:password@localhost:27017/')
        self.cnx = client[collection]
    
    def insert(self, collection, data):
        inserted = self.cnx[collection].insert_one(data)

        return inserted.inserted_id

    def select(self, collection, query=None):
        documents = self.cnx[collection].find(query)

        return documents

    def delete(self, collection, query=None):
        self.cnx[collection].delete_many(query)

    def _scape_string(self, input):
        new_value = str(input).replace("'", "\\'")
        return "'" + new_value + "'"

    def disconect_db(self):
        self.cnx.close()
