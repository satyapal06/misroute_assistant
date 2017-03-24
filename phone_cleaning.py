import csv
import re

std_code_list = []


def readSTDcode(filename):
	data = open(filename, 'rU')
	reader = csv.reader(data)
	keys = reader.next()
	for row in reader:
		record_dict = dict(zip(keys, row))
		# records_list.append(record_dict)
		std_code_list.append(record_dict['std code'])
	data.close()


def is_coherent(seq):
	in_check = ['987654', '876543', '765432', '654321', '123456', '234567', '345678', '456789']
	if seq == ''.join(str(i) for i in range(int(seq[0]), int(seq[0]) + len(seq), 1)):
		return True
	elif seq == ''.join(str(i) for i in range(len(seq), int(seq[0]) - 1, -1)):
		return True
	elif any(c in seq for c in in_check):
		return True
	else:
		return False


def address_phones(number_list):
	mobile = []
	landline = []
	check_index = []
	for i, num in enumerate(number_list):
		if i not in check_index:
			if len(num.lstrip('0')) == 10 and num.lstrip('0')[0] in ['9', '8', '7']:
				mobile.append(num.lstrip('0'))
			elif len(num.lstrip('0')) == 10 and any(
							num.lstrip('0')[len(std):][0] not in ['9', '8', '1', '0'] for
							std in std_code_list if num.lstrip('0').startswith(std)):
				landline.append(num.lstrip('0'))
			elif num.lstrip('0')[:2] == '91' and len(num.lstrip('0')[2:]) == 10 and num.lstrip('0')[2:][
				0] in ['9', '8', '7']:
				mobile.append(num.lstrip('0')[2:])
			elif num.lstrip('0')[:2] == '91' and len(num.lstrip('0')[2:]) == 10 and any(
							num.lstrip('0')[2:][len(std):][0] not in ['9', '8', '1', '0']
							for std in std_code_list if
							num.lstrip('0')[2:].startswith(std)):
				landline.append(num.lstrip('0')[2:])
			elif not num.lstrip('0').startswith('91') and 6 >= len(num.lstrip('0')) > 0 and i != len(
					number_list) - 1:
				if len(number_list[i + 1:]) >= 1 and num.lstrip('0') in std_code_list and \
								number_list[i + 1][0] not in ['9', '8', '1',
								                              '0'] and len(
					num.lstrip('0')) + len(number_list[i + 1]) == 10:
					landline.append(num.lstrip('0') + number_list[i + 1])
					check_index.append(i + 1)
				elif len(number_list[i + 1:]) >= 1 and num.lstrip('0')[0] in ['9', '8', '7'] and len(
						num.lstrip('0')) + len(number_list[i + 1]) == 10:
					mobile.append(num.lstrip('0') + number_list[i + 1])
					check_index.append(i + 1)
				elif len(number_list[i + 1:]) >= 2 and num.lstrip('0')[0] in ['9', '8', '7'] and len(
						num.lstrip('0')) + len(number_list[i + 1]) + len(
					number_list[i + 2]) == 10:
					mobile.append(num.lstrip('0') + number_list[i + 1] + number_list[i + 2])
					check_index.append(i + 1)
					check_index.append(i + 2)
				elif len(number_list[i + 1:]) >= 3 and num.lstrip('0')[0] in ['9', '8', '7'] and len(
						num.lstrip('0')) + len(number_list[i + 1]) + len(
					number_list[i + 2]) + len(number_list[i + 3]) == 10:
					mobile.append(
						num.lstrip('0') + number_list[i + 1] + number_list[i + 2] + number_list[
							i + 3])
					check_index.append(i + 1)
					check_index.append(i + 2)
					check_index.append(i + 3)
			elif num.lstrip('0').startswith('91') and 6 >= len(num.lstrip('0')[2:]) > 0 and i != len(
					number_list) - 1:
				if len(number_list[i + 1:]) >= 1 and num.lstrip('0')[2:] in std_code_list and \
								number_list[i + 1][0] not in ['9', '8', '1',
								                              '0'] and len(
					num.lstrip('0')[2:]) + len(number_list[i + 1]) == 10:
					landline.append(num.lstrip('0')[2:] + number_list[i + 1])
					check_index.append(i + 1)
				elif len(number_list[i + 1:]) >= 1 and num.lstrip('0')[2:][0] in ['9', '8',
				                                                                  '7'] and len(
					num.lstrip('0')[2:]) + len(number_list[i + 1]) == 10:
					mobile.append(num.lstrip('0')[2:] + number_list[i + 1])
					check_index.append(i + 1)
				elif len(number_list[i + 1:]) >= 2 and num.lstrip('0')[2:][0] in ['9', '8',
				                                                                  '7'] and len(
					num.lstrip('0')[2:]) + len(number_list[i + 1]) + len(
					number_list[i + 2]) == 10:
					mobile.append(num.lstrip('0')[2:] + number_list[i + 1] + number_list[i + 2])
					check_index.append(i + 1)
					check_index.append(i + 2)
				elif len(number_list[i + 1:]) >= 2 and num.lstrip('0')[2:][0] in ['9', '8',
				                                                                  '7'] and len(
					num.lstrip('0')[2:]) + len(number_list[i + 1]) + len(
					number_list[i + 2]) + len(number_list[i + 3]) == 10:
					mobile.append(num.lstrip('0')[2:] + number_list[i + 1] + number_list[i + 2] +
					              number_list[i + 3])
					check_index.append(i + 1)
					check_index.append(i + 2)
					check_index.append(i + 3)
			elif len(num.lstrip('0')) == 20:
				split_number_20 = re.findall('.{10}', num.lstrip('0'))
				for sn in split_number_20:
					if sn[0] in ['9', '8', '7']:
						mobile.append(sn)
					elif any(sn[len(std):][0] not in ['9', '8', '1', '0'] for std in std_code_list
					         if sn.startswith(std)):
						landline.append(sn)
	return mobile + landline


def clean_phone_number(number):
	cleaned_number = []
	rejected_number = []
	wsc_number = ''.join(n for n in number if n.isdigit())
	number_list = re.findall('\d+', number)

	check_index = []
	if len(wsc_number.lstrip('0')) - len(wsc_number.strip('0')) >= 5:
		rejected_number.append(number)

	elif is_coherent(wsc_number):
		rejected_number.append(number)

	elif len(set(wsc_number)) <= 2:
		rejected_number.append(number)

	elif len(wsc_number.lstrip('0')) >= 20:
		if ' ' in number or '-' in number:
			mob_land = address_phones(number_list)
			if mob_land:
				cleaned_number.extend(mob_land)
			else:
				rejected_number.append(number)
		else:
			if len(wsc_number.lstrip('0')) == 30:
				split_number_30 = re.findall('.{10}', wsc_number.lstrip('0'))
				for sn in split_number_30:
					if sn[0] in ['9', '8', '7']:
						cleaned_number.append(sn)
					elif any(sn[len(std):][0] not in ['9', '8', '1', '0'] for std in std_code_list
					         if sn.startswith(std)):
						cleaned_number.append(sn)
					else:
						rejected_number.append(sn)

			elif len(wsc_number.lstrip('0')) == 20:
				split_number_20 = re.findall('.{10}', wsc_number.lstrip('0'))
				for sn in split_number_20:
					if sn[0] in ['9', '8', '7']:
						cleaned_number.append(sn)
					elif any(sn[len(std):][0] not in ['9', '8', '1', '0'] for std in std_code_list
					         if sn.startswith(std)):
						cleaned_number.append(sn)
					else:
						rejected_number.append(sn)
			else:
				rejected_number.append(number)

	elif any(wsc_number.count(i) >= 8 for i in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']):
		rejected_number.append(number)

	elif len(wsc_number.lstrip('0')) >= 10:
		if wsc_number.lstrip('0')[0] in ['9', '8', '7'] and len(wsc_number.lstrip('0')) == 10:
			cleaned_number.append(wsc_number.lstrip('0'))
		elif wsc_number.lstrip('0')[:4] == '1800' and len(wsc_number.lstrip('0')) == 11:
			cleaned_number.append(wsc_number.lstrip('0'))
		elif any(wsc_number.lstrip('0')[len(std):][0] not in ['9', '8', '1', '0'] for std in std_code_list if
		         wsc_number.lstrip('0').startswith(std) and len(wsc_number.lstrip('0')[len(std):]) == 10 - len(
			         std)):
			cleaned_number.append(wsc_number.lstrip('0'))
		elif any(wsc_number.lstrip('0')[len(std):][0] in ['9', '8', '7'] for std in std_code_list if
		         wsc_number.lstrip('0').startswith(std) and len(wsc_number.lstrip('0')[len(std):]) == 10):
			cleaned_number.append(wsc_number.lstrip('0')[-10:])
		elif any(wsc_number.lstrip('0')[len(std):][:2] == '91' and wsc_number.lstrip('0')[len(std):][2:][0] in [
			'9', '8', '7'] for std in std_code_list if
		         wsc_number.lstrip('0').startswith(std) and len(wsc_number.lstrip('0')[len(std):][2:]) == 10):
			cleaned_number.append(wsc_number.lstrip('0')[-10:])
		elif wsc_number.lstrip('0')[:2] == '91':
			if wsc_number.lstrip('0')[2:].lstrip('0')[0] in ['9', '8', '7'] and len(
					wsc_number.lstrip('0')[2:].lstrip('0')) == 10:
				cleaned_number.append(wsc_number.lstrip('0')[2:].lstrip('0'))
			elif wsc_number.lstrip('0')[2:].lstrip('0')[:4] == '1800' and len(
					wsc_number.lstrip('0')[2:].lstrip('0')) == 11:
				cleaned_number.append(wsc_number.lstrip('0')[2:].lstrip('0'))
			elif any(wsc_number.lstrip('0')[2:].lstrip('0')[len(std):][0] not in ['9', '8', '1', '0'] for
			         std in std_code_list if wsc_number.lstrip('0')[2:].lstrip('0').startswith(std) and len(
				wsc_number.lstrip('0')[2:].lstrip('0')) == 10):
				cleaned_number.append(wsc_number.lstrip('0')[2:].lstrip('0'))
			elif any(wsc_number.lstrip('0')[2:].lstrip('0')[len(std):][0] in ['9', '8', '7'] for std in
			         std_code_list if wsc_number.lstrip('0')[2:].lstrip('0').startswith(std) and len(
				wsc_number.lstrip('0')[2:].lstrip('0')[len(std):]) == 10):
				cleaned_number.append(wsc_number.lstrip('0')[2:].lstrip('0')[-10:])
			elif ' ' in number or '-' in number:
				mob_land = address_phones(number_list)
				if mob_land:
					cleaned_number.extend(mob_land)
				else:
					rejected_number.append(number)
			else:
				rejected_number.append(number)
		elif ' ' in number or '-' in number:
			mob_land = address_phones(number_list)
			if mob_land:
				cleaned_number.extend(mob_land)
			else:
				rejected_number.append(number)
		else:
			rejected_number.append(number)
	else:
		rejected_number.append(number)

	return list(set(cleaned_number)), list(set(rejected_number))


def extract_number(phone_string):
	phone_list = [pn.strip() for pn in re.findall('[\d\s\-]+', phone_string) if
	              len(''.join(e for e in pn if e.isdigit())) > 6]
	if phone_list:
		cleaned = []
		rejected = []
		for phone in phone_list:
			clean_list, reject_list = clean_phone_number(phone)
			cleaned.extend(clean_list)
			rejected.extend(reject_list)
		return cleaned, rejected
	else:
		return [], []
