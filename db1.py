import sys
import math
import pandas as pd
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask import render_template
from sqlalchemy.ext.hybrid import hybrid_method, hybrid_property
from sqlalchemy import func
df = pd.read_csv('IN.csv')

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:12345@localhost/mydatabase'

db = SQLAlchemy(app)

class Pincode(db.Model):
    
    __tablename__ = 'pincode'

    key = db.Column(db.String(100), primary_key = True)
    place_name = db.Column(db.String(150))
    admin_name1 = db.Column(db.String(100))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    accuracy = db.Column(db.Float)

    def __init__(self, key, place_name, admin_name1, latitude, longitude, accuracy):

        self.key = key
        self.place_name = place_name
        self.admin_name1 = admin_name1
        self.latitude = latitude
        self.longitude = longitude
        self.accuracy = accuracy
    


    @hybrid_method
    def great_circle_distance(self, p, l):

        radius = 6371 # Radius of earth
    
        p1 = self.latitude
        l1 = self.longitude

        print(p1, l1)

        p2 = p

        l2 = l

        dl = math.radians(abs(l1 - l2))

        p1 = math.radians(p1)
        p2 = math.radians(p2)
        l1 = math.radians(l1)
        l2 = math.radians(l2)

        ds = math.acos((math.sin(p1) * math.sin(p2)) + (math.cos(p1) * math.cos(p2) * math.cos(dl)))

        dist = radius * ds

        return dist
        
    @great_circle_distance.expression
    def great_circle_distance(cls, p, l):

        radius = 6371 # Radius of earth
    
        p1 = cls.latitude
        l1 = cls.longitude

        print(p1, l1)

        p2 = p

        l2 = l
 
        dl = func.radians(func.abs(l1 - l2))

        p1 = func.radians(p1)
        p2 = func.radians(p2)
        l1 = func.radians(l1)
        l2 = func.radians(l2)
        
        
        ds = func.acos((func.sin(p1) * func.sin(p2)) + (func.cos(p1) * func.cos(p2) * func.cos(dl)))

        dist = radius * ds

        return dist
        


@app.route('/post_location', methods = ['POST'])
def post_location():
    
    data = request.get_json()

    if 'key' not in data:
        
        return 'Error! Incomplete data.\n', 400
    
    key_value = data['key']

    if Pincode.query.get(key_value):
        
        print('Record already exists! \n', file = sys.stderr)

        return 'Error! Already present.\n', 400

    pin = Pincode(data['key'], data['place_name'], data['admin_name1'], data['latitude'], data['longitude'], data['accuracy'])
    db.session.add(pin)

    print('Success!\n', file = sys.stderr)

    return 'Success! Record added.\n', 200

@app.route('/get_using_self', methods = ['GET', 'POST'])
def get_using_self():

    data = request.get_json()

    lat = data['latitude']    

    lon = data['longitude']
    
    out = Pincode.query.filter(Pincode.great_circle_distance(lat, lon) <= 5).all()

    tem = []

    for a in out:
        tem.append(str(a.key).strip(',').strip(')').strip('('))

    tems = ''.join(tem)
    
    return tems, 200


@app.route('/get_using_postgres', methods = ['GET', 'POST'])
def get_using_postgres():

    data = request.get_json()

    lat = data['latitude']
    lon = data['longitude']

    query = f'SELECT key FROM pincode WHERE earth_distance(ll_to_earth({lat}, {lon}), ll_to_earth(pincode.latitude, pincode.longitude)) <= 5000'

    out = db.engine.execute(query)
    
    near = []

    for i in out:
        near.append(str(i).strip(',').strip('(').strip(')'))

    outm = ''.join(near)

    return outm, 200

if __name__ == '__main__':
    db.create_all()
    
    print('start')
    df.to_sql(name = 'pincode', con = db.engine, schema = 'public', index = False, if_exists = 'append', chunksize = 100)
    print('end')
    
    app.run(debug = True, use_reloader = False)

