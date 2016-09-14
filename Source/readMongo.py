import pprint
from pymongo import MongoClient

client = MongoClient()
my_db = client.custdb
col = my_db.cust_details

list_of_adr = []  # we will use this to store the details obtained from MongoDB
cursor = col.find()
count = 0
all_record_types = []
for document in cursor:
	if "9535751827" in (document["phone_number"]):
		count += 1
		pprint.pprint(document)
		print "\n\n\n"
		if count == 30:
			break
			# 	for record in document['address_components']:
			# 		for rec_type in record['types']:
			# 			all_record_types.append(rec_type)
			# client.close()
			#
			# all_record_types = list(set(all_record_types))
			# pprint.pprint(all_record_types)
