from flask import Flask, jsonify, request #import objects from the Flask model
import json,time,os,sys
from datetime import datetime
import pickle

app = Flask(__name__) #define app using Flask

cwd = os.getcwd()
if len(sys.argv) > 1:
	restart = sys.argv[1]
else:
	restart = "ColdStart"
	
if restart == "StartFromBackup":
	with open('./backupdb/backupdatabase.p', 'rb') as f: D = pickle.load(f)
else:
	D = {"data": {}}

@app.route('/getmydata/<theKey>/', methods=['GET'])
def getData(theKey):
    if theKey in D["data"].keys():
        return jsonify(D["data"][theKey])
    else:
        return jsonify({"customer_id": theKey,
                        "status" : "No Data"})
    

@app.route('/putmydata/<theKey>/',methods=['POST'])
def putData(theKey):
    
    if theKey not in D["data"].keys():
        D["data"][theKey] = { "CUSTOMER" :{"last_name":"" , "adr_city": "","adr_state":""},
                            "SITE_VISIT":0 ,
                            "SITE_VISIT_DETAIL" : {},
                            "IMAGE":0,
                            "IMAGE_DETAIL" :{},
                            "ORDER":{"total_amount":0,"order_count":0},
                            "ORDER_DETAIL" : {}
                             }

    if request.get_json().get("type") == "CUSTOMER":         #verb  NEW   UPDATE
        D["data"][theKey]["CUSTOMER"]["last_name"] = request.get_json().get("last_name")
        D["data"][theKey]["CUSTOMER"]["adr_city"] = request.get_json().get("adr_city")
        D["data"][theKey]["CUSTOMER"]["adr_state"] = request.get_json().get("adr_state")
        D["data"][theKey]["CUSTOMER"]["event_time"] = request.get_json().get("event_time")
        
        with open(cwd+"\\logs\\"+theKey+"-"+str(time.time())+".log","w") as x: x.write(str(request.get_json()))
        

    return json.dumps(D)
  

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080) #run app on port 8080 in debug mode