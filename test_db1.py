import unittest
from flask import Flask, json
from db1 import db, app, df
from flask_testing import TestCase

class Testdb1(TestCase):

    SQLALCHEMY_DATABASE_URI = 'postgresql://postgres:12345@localhost/mydatabase'
    TESTING = True


    def create_app(self): 
        app.config['TESTING'] = True
        return app

    def setUp(self):
        db.create_all()
        df.to_sql(name = 'pincode', con = db.engine, schema = 'public', index = False, if_exists = 'append', chunksize = 100)

    def test_post_location(self):
        tester = app.test_client(self)

        response = tester.post('/post_location', data = json.dumps(dict(key = '48604', place_name = 'patna', admin_name1 = 'Siddharth', latitude = 22.5, longitude = 77.5, accuracy = 4)), content_type = 'application/json')

        response = tester.post('/post_location', data = json.dumps(dict(key = 'IN/110001', place_name = 'patna', admin_name1 = 'Siddharth', latitude = 22.5, longitude = 77.5, accuracy = 4)), content_type = 'application/json')



        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Success! Record added.\n', response.data) 
    
    
    def tearDown(self):

        db.session.remove()
        db.drop_all()
    


if __name__ == '__main__':
    unittest.main()
