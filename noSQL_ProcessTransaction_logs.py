import json,os,ast,shutil,pickle,time
from datetime import datetime

def Ingest(e, D):
	
	# Ingest function for adding data to the data structure
	
	if e["type"] == "CUSTOMER":
		theKey = e["key"]
	else:
		theKey = e["customer_id"]
		if theKey == "":
			print("Not a valid customer id : "  + e)
			return 
	
	#Getting the event time into a variable and computing min and max date for the dataset
	evantDtt = datetime.strptime(e["event_time"], "%Y-%m-%dT%H:%M:%S.%fZ")

	#Since records can be recieved in any order we are adding customerID to the datastructure for not missing the event
	if theKey not in D["data"].keys():
		
		D["data"][theKey] = { "CUSTOMER" :{"last_name":"" , "adr_city": "","adr_state":"","event_time":datetime.min,"joinDate": datetime.max},
							"SITE_VISIT":0 ,
							"SITE_VISIT_DETAIL" : {},
							"IMAGE":0,
							"IMAGE_DETAIL" :{},
							"ORDER":{"total_amount":0,"order_count":0},
							"ORDER_DETAIL" : {}
							 }
	
	if e["type"] == "CUSTOMER":		 #verb  NEW   UPDATE
			# event time drive if the data needs to be updated.
			# record will not be updated If new timestamp is older than the exisiting record
			if evantDtt > D["data"][theKey]["CUSTOMER"]["event_time"]:
				D["data"][theKey]["CUSTOMER"]["last_name"] = e["last_name"]
				D["data"][theKey]["CUSTOMER"]["adr_city"] = e["adr_city"]
				D["data"][theKey]["CUSTOMER"]["adr_state"] = e["adr_state"]
				D["data"][theKey]["CUSTOMER"]["event_time"] = evantDtt
				D["data"][theKey]["CUSTOMER"]["joinDate"] = min(D["data"][theKey]["CUSTOMER"]["joinDate"],evantDtt)
			else:
				#If the evantDtt is < than the current event date then the join Date is update for LTV computation
				D["data"][theKey]["CUSTOMER"]["joinDate"] = min(D["data"][theKey]["CUSTOMER"]["joinDate"],evantDtt)
			
	elif e["type"] == "SITE_VISIT":	 #verb NEW
		#update the number of visit counter 
		D["data"][theKey]["SITE_VISIT"] += 1
		#Add individual records
		D["data"][theKey]["SITE_VISIT_DETAIL"]["key"] = e["key"]
		D["data"][theKey]["SITE_VISIT_DETAIL"]["event_time"] = evantDtt
		D["data"][theKey]["SITE_VISIT_DETAIL"]["tags"] = e["tags"]
		
	elif e["type"] == "IMAGE":		  #verb UPLOAD
		#update the number of image upload counter
		D["data"][theKey]["IMAGE"] += 1
		#Add individual records
		D["data"][theKey]["IMAGE_DETAIL"]["key"] = e["key"]
		D["data"][theKey]["IMAGE_DETAIL"]["event_time"] = evantDtt
		D["data"][theKey]["IMAGE_DETAIL"]["camera_make"] = e["camera_make"]
		D["data"][theKey]["IMAGE_DETAIL"]["camera_model"] = e["camera_model"]
		
	elif e["type"] == "ORDER":		  #verb NEW   UPDATE
		
		#take only the numeric value in the total_amount e.g "12.34 USD", 12.34 will be saved to the datastructure
		try:
			total_amount = float(e["total_amount"].split(" ")[0])
		except:
			print("Order total amount not valid :" +e)
			return 
		
		if e["key"] not in D["data"][theKey]["ORDER_DETAIL"].keys():
			#If this order is recieved for the first time. the order Key , total_amount and event date time is saved to the data structure
			D["data"][theKey]["ORDER_DETAIL"][ e["key"]] = (total_amount,evantDtt)
			D["data"][theKey]["ORDER"]["order_count"] += 1
			D["data"][theKey]["ORDER"]["total_amount"] += total_amount
		elif evantDtt > D["data"][theKey]["ORDER_DETAIL"][ e["key"]][1] :
				#if the event date is greater than saved record ORDER total_amount is updated 
				D["data"][theKey]["ORDER"]["total_amount"] =  D["data"][theKey]["ORDER"]["total_amount"] \
																	+ (total_amount - D["data"][theKey]["ORDER_DETAIL"][ e["key"]][0])
					
				#Order detail record is pdated based on the event key of the ORDER event
				D["data"][theKey]["ORDER_DETAIL"][ e["key"]] = (total_amount,evantDtt)
 
	else:
		print("Data not mapped! : " + e)
		return
	  
if __name__ == "__main__":

	D = {"data": {}}
	baseLogFolder = './logs/' 
	baseProcessedLogFolder = "./processedlogs/"
	
	if os.path.exists("./backupdb/backupdatabase.p"):
		with open('./backupdb/backupdatabase.p', 'rb') as f: D = pickle.load(f)
		os.rename("./backupdb/backupdatabase.p","./backupdb/backupdatabase.p_"+str(time.time()))
		
	logfiles = [filename.replace(".log","").split("-") for filename in os.listdir(baseLogFolder)]
	logfiles.sort(key=lambda x:x[1])
	
	for logfile in logfiles:
		logfile = logfile[0] + "-" + logfile[1] + ".log"
		tplogfile = baseLogFolder + logfile
		with open(tplogfile) as x: inData = x.read()
		#inData = (inData.strip())
		#toProcessData = json.loads(json.dumps(inData))
		#Make the logs as a JSON formated file
		toProcessData = ast.literal_eval(inData)
		Ingest(toProcessData,D)
		shutil.move(tplogfile,baseProcessedLogFolder+logfile)
		
	pickle.dump( D, open( "./backupdb/backupdatabase.p", "wb" ) )