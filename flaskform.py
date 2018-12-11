from flask import Flask, render_template, request
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import uuid
import signal
import sys
import datetime
import json
import redis

cluster = Cluster()
session = cluster.connect('testcluster')
session.row_factory = dict_factory
asset_lookup_stmt = session.prepare("SELECT json * FROM asset WHERE asset_owner_emailid=?")

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

app = Flask(__name__,static_url_path='/static')


def signalHandler(signum, frame):
    print("Ctrl-C pressed..closing connection to cassandra before shutting down..")
    cluster.shutdown()
    sys.exit(0)


@app.route('/')
def assetform():
	return render_template('itassets.html')

@app.route('/postasset',methods = ['POST'])
def result():
	if request.method == 'POST':
		result = request.form.to_dict()
		insertasset(result)
		print(result)
		return render_template("result.html",result = result)


@app.route('/search')
def search():
	return render_template('search_emp_assets.html')

@app.route('/searchresult',methods = ['POST'])
def searchresult():
	if request.method == 'POST':
		emp = request.form.to_dict()
		result, resultfrom = searchasset(emp)
		#print("%s %s",type(result),type(resultfrom))
		return result + "\n\n" + resultfrom
		#return render_template("result.html",result = result)

#def validateasset(result):

def insertasset(result):
	assetid = uuid.uuid4()
	session.execute(
		"""
		INSERT INTO asset (asset_id,asset_cat,asset_condition,asset_cost,asset_desc,asset_loaned_date,asset_model,asset_owner_emailid,asset_purchase_date,asset_serial,asset_warrantyexp_date)
		VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(assetid,result['assetCategory'],result['condition'],float(result['assetCost']),result['assetDescription'],datetime.datetime.strptime(result['dateLoaned'], "%m/%d/%Y").date().strftime("%Y-%m-%d"),result['modelNo'],result['assetOwnerEmailId'],datetime.datetime.strptime(result['datePurchased'], "%m/%d/%Y").date().strftime("%Y-%m-%d"),result['serialNo'],datetime.datetime.strptime(result['dateWarrantyExp'], "%m/%d/%Y").date().strftime("%Y-%m-%d")))
	if r.exists(result['assetOwnerEmailId']):
		updatecache(result['assetOwnerEmailId'],empassets=[])


def searchasset(emp):
	print(emp['emp_emailid'])
	empassets = r.get(emp['emp_emailid'])
	resultfrom = "Result returned from redis."
	if empassets is None:
		result = session.execute(asset_lookup_stmt,(emp['emp_emailid'],))
		empassets = []
		resultfrom = "Result returned from cassandra."
		for results in result:
			empassets.append(results)
		empassets = json.dumps(empassets,indent=4,sort_keys=True)
		if empassets:
			updatecache(emp['emp_emailid'],empassets)
	return empassets, resultfrom
		

def updatecache(emp,empassets):
	if not empassets:
		result = session.execute(asset_lookup_stmt,(emp,))
		for results in result:
			empassets.append(results)
		empassets = json.dumps(empassets,indent=4,sort_keys=True)
	r.set(emp,empassets)


if __name__ == '__main__':
	signal.signal(signal.SIGINT,signalHandler);
	app.run(debug = True)
