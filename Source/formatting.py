import re
import csv

list_of_special_codes_to_be_removed = [
	#       This list contains all the values that we want to remove from
	#       the address string
	"\u003eP",
	"\u003e",
	"\u0026V",
	"\u0026t",
	"\u0026S",
	"\u0026p",
	"\u0026M",
	"\u0026H",
	"\u0026F",
	"\u0026E",
	"\u0026C",
	"\u0026B",
	"\u0026ampHD",
	"\u0026ampDD",
	"\u0026amp;",
	"\u0026amp",
	"\u0026agrave;",
	"\u0026ADD",
	"\u0026aacute;",
	"\u0026a",
	"\u0026;",
	"\u0026#xa;",
	"\u0026#x2f;",
	"\u0026#x29;",
	"\u0026#x28;",
	"\u0026#x24;",
	"\u0026#40;",
	"\u0026#341;",
	"\u0026",
	"\u0009P",
	"\u0009",
	"u0026",
	"?",
	"[",
	"]",
	"\"",
	"\'",
	")",
	"(",
	",",
	".",
	"-",
	":",
	"/",
	"^",
	"#"
]


def mergeAbbreviations(address):
	# PURPOSE:
	#       This function accepts an address and looks for abbreviations.
	#       All the abbreviations are then shortened. This is done to improve
	#       the accuracy of the token_set fuzzy search as it reduces the number
	#       of words in the address

	#       The purpose of below code block is to look for abbreviations in the 
	#       beginning of the address and consolidate it
	start_regex = re.compile(r'^([a-z][\s])+')
	start_regex_match = start_regex.search(address)
	if start_regex_match is not None:
		start_regex_match = start_regex.search(address).group()
		start_regex_match_new = str(start_regex_match).replace(" ", "") + " "
		address = address.replace(start_regex_match, start_regex_match_new)

	#       The purpose of below code block is to look for abbreviations in the
	#       end of the address and consolidate it
	end_regex = re.compile(r'([\s][a-z])+$')
	end_regex_match = end_regex.search(address)
	if end_regex_match is not None:
		end_regex_match = end_regex.search(address).group()
		end_regex_match_new = " " + str(end_regex_match).replace(" ", "")
		address = address.replace(end_regex_match, end_regex_match_new)

	#       The purpose of below code block is to look for abbreviations in the
	#       middle of the address and consolidate it
	while True:
		middle_regex = re.compile(r' ([a-z][\s])+')
		middle_regex_match = middle_regex.search(address)
		if middle_regex_match is not None and len(middle_regex.search(address).group()) > 4:
			#       Only those abbreviations which have at-least 2 alphabets 
			#       example: " c o " are consolidated, hence the len > 4. Otherwise,
			#       the while code block will go into an infinite loop.
			middle_regex_match = middle_regex.search(address).group()
			middle_regex_match_new = " " + str(middle_regex_match).replace(" ", "") + " "
			address = address.replace(middle_regex_match, middle_regex_match_new)
		else:
			break

	return address


def formatAddress(address):
	new_adr = str(address)
	for code in list_of_special_codes_to_be_removed:
		new_adr = new_adr.replace(code, " ")
	new_adr = str(new_adr).replace("\\n", " ")
	while "  " in new_adr:
		new_adr = new_adr.replace("  ", " ")
	new_adr = new_adr.strip()
	new_adr = new_adr.lower()
	new_adr = mergeAbbreviations(new_adr)
	return new_adr


def formatPhone(number_list):
	new_number_list = number_list
	space_regex = re.compile(r'[0-9+]{10,15} [0-9+]{10,15}')
	if "," in str(new_number_list):
		new_number_list = str(new_number_list).split(",")
	elif "/" in str(new_number_list):
		new_number_list = str(new_number_list).split("/")
	elif ";" in str(new_number_list):
		new_number_list = str(new_number_list).split(";")
	elif space_regex.search(str(new_number_list)):
		new_number_list = str(new_number_list).split(" ")
	else:
		new_number_list = [number_list]

	number_list = []
	for number in new_number_list:
		new_number = str(number).replace("[", "").replace("]", "").replace("\"", "")
		new_number = new_number.replace(" ", "").replace(" ", "").replace("-", "")
		new_number = new_number.replace(".", "")
		number_list.append(new_number)
	phone_regex = re.compile(r'[0-9+]{10,15}')
	list_of_formatted_numbers = []

	for num in number_list:
		mo = phone_regex.findall(str(num))
		if mo:  # If its a phone number
			num = mo[0]
			if len(num) == 10:
				list_of_formatted_numbers.append(num)

			elif len(num) > 10:
				if num[0:2] == "00" and len(num) > 12 and num[len(num) - 10] in ['9', '8', '7']:
					num = num[len(num) - 10: len(num)]
				if num[0:3] == "+91" and len(num) == 13:
					new_num = num[3: len(num)]
					list_of_formatted_numbers.append(new_num)
				elif num[0:3] == "091" and len(num) == 13:
					new_num = num[3: len(num)]
					list_of_formatted_numbers.append(new_num)
				elif num[0:2] == "91" and len(num) == 12:
					new_num = num[2: len(num)]
					list_of_formatted_numbers.append(new_num)
				elif num[0] == "0" and len(num) == 11 and num[1] in ['9', '8', '7']:
					new_num = num[1: len(num)]
					list_of_formatted_numbers.append(new_num)
				elif len(num) > 10:
					list_of_formatted_numbers.append(num)

	return list_of_formatted_numbers

# def readCustomerDataFromCSV(filename, records_list):
# 	# PURPOSE:
# 	#       This function reads the contents of customer data from the CSV file
# 	# ARGUMENTS:
# 	#       "filename": This is the name of the file from where data has to read
# 	#       "records_list": This is an empty list into which newly read data will be added
# 	# Its better to call the records_list by reference than to return it
# 	data = open(filename, 'rU')
# 	reader = csv.reader(data)
# 	keys = reader.next()
# 	for row in reader:
# 		record_dict = dict(zip(keys, row))
# 		records_list.append(record_dict)
# 	data.close()


# def writeToCSV(filename, contents):
# 	# PURPOSE:
# 	#       This function reads the contents of a List of Dictionaries, and stores
# 	#       them in a csv file.
# 	with open(filename, 'wb') as f:
# 		w = csv.DictWriter(f, contents[0].keys())
# 		w.writeheader()
# 		for row in contents:
# 			w.writerow(row)
# 	return True


# if __name__ == '__main__':
# 	records_list = []
# 	final_list = []
# 	readCustomerDataFromCSV('data.csv', records_list)
# 	for row in records_list:
# 		myDict = {
# 			"Original Phone": str(row['ph']),
# 			"Formatted Phone": str(formatPhone(row['ph'])),
# 			"Original Address": str(row['add']),
# 			"Formatted Add": formatAddress(row['add'])
# 		}
# 		final_list.append(myDict)
#
# 	status = False
# 	while status is False:
# 		try:
# 			status = writeToCSV("formatting.csv", final_list)
# 		except IOError as e:
# 			ch = raw_input("Close the file <formatting.csv> and press any key to continue: ")
