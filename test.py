import unittest
from main import MongoDBManagement
import logging


class TestMongoDB(unittest.TestCase):

    def setUp(self):
        import os
        self.BASE_PATH = os.path.dirname(os.path.abspath(__file__))
        self.conection = MongoDBManagement()
        self.conection.connect_db("complaints")

    # def test_csv_read(self):
    #     filepath = self.BASE_PATH + "/mockups/complaints.csv"
    #     processor = TextProcessor()
    #     processor.read_csv(filepath)

    def test_insert_reclamo(self):
        import datetime
        reclamo = {
            "Data_ricevuta": datetime.datetime.now(),
            "Prodotto": "Prodotto esempio",
            "Sottoprodotto": "Sottoprodotto esempio",
            "Problema": "Cosa da sistemare",
            "Sottoproblema": None, 
            "Narrativa": "blòah blahy balhy",
            "Risposta_publica": "tutto apposto",
            "Ditta": {
                "Paese": "Italia",
                "Cap": "34601",
                "Tags": []
            },
            "consenso_cliente": True,
            "Inviato_via": "mail",
            "Data_invio": "2020-02-18",
            "Risposta_cliente": "Vabeh",
            "Risposta_tempestiva": False,
            "Cliente_contestato": True,
            "Cliente": {
                "Nome": "Fausto",
                "Eta": 24,
                "Data_registrazione": "2019-12-05",
                "Indirizzi": [
                    {"strada": "una", "numero": 45},
                    {"strada": "due", "numero": 1}
                ]
            }
        }
        inserted_doc = self.conection.insert("complaints", reclamo)
        # result = self.conection.cnx['complaints'].find_one()

    def test_erase_data(self):
        all_doc = self.conection.select("complaints")

        for doc in all_doc:
            self.conection.delete("complaints", query={'_id':doc.get('_id')})

    def tearDown(self):
        try:
            self.conection.disconect_db()
        except Exception:
            logging.warning("Non si è potuta chiudere la conesione, forse era già chiusa?")

