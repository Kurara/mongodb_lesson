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

    def test_insert_cliente(self):
        
        import datetime
        data = {
            "Nome": "Primo cliente",
            "Indirizzo": "Strada principale",
            "Eta": 34,
            "Data_registrazione": datetime.datetime.now().isoformat()
        }
        self.conection.insert("complaints", data)
        result = self.conection.cnx['complaints'].find_one()
        self.conection.disconect_db()

    def tearDown(self):
        try:
            self.conection.disconect_db()
        except Exception:
            logging.warning("Non si è potuta chiudere la conesione, forse era già chiusa?")

