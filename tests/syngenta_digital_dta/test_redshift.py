import unittest
import warnings

import syngenta_digital_dta

class RedshiftAdapterTest(unittest.TestCase):

    def setUp(self, *args, **keywargs):
        warnings.simplefilter("ignore", ResourceWarning)
        self.maxDiff = None
        TABLE_NAME='unittestsort'
        self.adapter_1 = syngenta_digital_dta.adapter(
            engine='redshift',
            endpoint='dta-test-cluster.covqcsgpxbmn.us-east-2.redshift.amazonaws.com',
            database='dta',
            table=TABLE_NAME,
            port=5439,
            user='root',
            password='Lq4nKg&&TRhHv%7z'
        )
        self.adapter_2 = syngenta_digital_dta.adapter(
            engine='redshift',
            endpoint='dta-test-cluster.covqcsgpxbmn.us-east-2.redshift.amazonaws.com',
            database='dta',
            table=TABLE_NAME,
            port=5439,
            user='root',
            password='Lq4nKg&&TRhHv%7z'
        )

    def test_connect(self):
        self.adapter_1.connect()
        self.adapter_2.connect()
