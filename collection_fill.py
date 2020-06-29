import mysql.connector
from pymongo import MongoClient
import datetime
from . import main


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
        _cursor = self.cnx.cursor(buffered=True)
        rows = None
        try:
            _cursor.execute(query)
            rows = _cursor.fetchmany(size=200)
        except Exception as e:
            print("Error executing statement select")
            print(e)
        
        return _cursor.description, rows

    def disconect_db(self):
        self.cnx.close()


if __name__ == "__main__":

    mariadb = MariaDBManagement()
    mariadb.connect_db()

    conection = main.MongoDBManagement()
    conection.connect_db("complaints")

    fields, db_rows = mariadb.select("""
        SELECT r.*, d.*, c.* FROM complaints.Ditte d
            RIGHT JOIN complaints.Reclami r ON r.Ditta_id = d.Id 
	            LEFT JOIN complaints.Clienti c ON r.Cliente_id = c.id """
    )
    ditta_fields = ["Paese", "Cap", "Nome"]
    clienti_fields = ["Nome", "Eta", "Data_registrazione"]
    for _row in db_rows:
        to_insert = {}
        ditta = {}
        cliente = {}
        for idx in range(len(_row)):
            # Prendere il nome del campo prima:
            field_name = fields[idx][0]
            if field_name.lower() == 'id':
                continue
            # Come 'Nome' apare sia nella ditta che nel cliente,
            # secondo la query che abbiamo fatto, la ditta apare per prima
            # quindi la prima volta 'Nome' corrisponde a ditta
            if field_name in ditta_fields:
                if field_name not in ditta.keys():
                    ditta[field_name] = _row[idx]
                else:
                    cliente[field_name] = _row[idx]
            elif field_name in clienti_fields:
                cliente[field_name] = _row[idx]
            else:
                if isinstance(_row[idx], datetime.date):
                    full_date = datetime.datetime(_row[idx].year, _row[idx].month, _row[idx].day)
                    to_insert[field_name] = full_date
                else:
                    to_insert[field_name] = _row[idx]

        # vado a prendermi il dato degl'indirizzi
        if to_insert.get('Cliente_id') is not None and to_insert.get('Cliente_id') != '':
            indirizzi = []
            qry = """SELECT i.* FROM complaints.Indirizzi i
                    INNER JOIN complaints.clienti_indirizzi ci ON ci.Indirizzo_id = i.id
                        WHERE ci.Cliente_id = {} """.format(to_insert.get('Cliente_id'))
            ind_fields, ind_rows = mariadb.select(qry)
            if ind_rows:
                for _irow in ind_rows:
                    _ind = {}
                    for idx in range(len(_irow)):
                        field_name = ind_fields[idx][0]
                        if field_name.lower() != "id":
                            _ind[field_name] = _irow[idx]
                    indirizzi.append(_ind)
            cliente['Indirizzi'] = indirizzi
        to_insert['Cliente'] = cliente
        to_insert['Ditta'] = ditta
        to_insert.pop('Cliente_id')
        conection.insert('complaints', to_insert)
    mariadb.disconect_db()
