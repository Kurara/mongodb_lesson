from .main import MongoDBManagement

class MariaDBManagement:
    
    """ Global functions
    """
    def connect_db(self, database=None):
        if database:
            self.cnx = mysql.connector.connect(
                user='root',
                password='password',
                host='172.17.0.2',
                database=database
            )
        else:
            self.cnx = mysql.connector.connect(
                user='root',
                password='password',
                host='172.17.0.2'
            )

    def select(self, query):
        _cursor = self.cnx.cursor()
        rows = None
        try:
            _cursor.execute(query)
            rows = _cursor.fetchmany(size=200)
        except Exception as e:
            print("Error executing statement select")
            print(e)
        
        return rows

    def disconect_db(self):
        self.cnx.close()


if __name__ == "__main__":
    mariadb = MariaDBManagement()
    mariadb.connect_db()
    db_rows = mariadb.select("""
        SELECT * FROM """
    )
    mariadb.disconect_db()

    self.conection = MongoDBManagement()
    self.conection.connect_db("complaints")

    for _row in db_rows:
