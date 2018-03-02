import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import string

OSMFILE = "ex_h59MB33V6XrsLWjzXhs7CWHwY3NCz.osm"

phone_numbers = []

#phone_numbers_re = re.compile(r"^\+\d{1,2}")
#phone_numbers_re = re.compile(r"^([+0-9][0-9]{1,5})(?:\s|)\d{1,5}(?:\s)(\d+)((?:\s|-)([0-9a-zA-Z]+))$")
phone_numbers_re = re.compile(r"^([+0-9]{3}|[0-9]{3,5})\s[0-9]")

def audit_phone_numbers(phone_number):

    m = phone_numbers_re.search(phone_number)
    if not m:
        edit_phone_number(phone_number)


def is_phone_number(elem):
    return (elem.tag == "tag") and (elem.attrib["k"] == "phone")


def audit():
    osm_file = open(OSMFILE, encoding='utf8')
    for event, elem in ET.iterparse(osm_file):
        if is_phone_number(elem):
            audit_phone_numbers(elem.attrib["v"])
    osm_file.close()


def edit_phone_number(phone_number):

    #remove all non-digit characters except plus and minus sign
    non_decimal = re.compile(r"[^\d\s+\s-]")
    phone_number = non_decimal.sub(" ", phone_number)

    #remove duplicate spaces
    phone_number = re.sub(' +', ' ', phone_number)

    #check if minus sign is between area code and phone number, if yes, remove it
    phone_numbers_re2 = re.compile(r"^([+0-9]{3}|[0-9]{3,5})-")
    m1 = phone_numbers_re2.search(phone_number)
    if m1:
        phone_number = phone_number.replace("-", " ", 1)

    #remove space at the beginning of phone number

    if phone_number[0] == " ":
        phone_number = phone_number.replace(" ", "", 1)


    #check if space is between area code and phone number, if not, insert it
    #list with fixed prefixes
    prefixes_fixed = ["9131 ", "9135 ", "911 ", "9133 ", "320 ", "700 ", "800 ", "900 ", "1511 ", "1512 ", "1514 ",
                      "1515 ", "1517 ", "160 ", "170 ", "171 ", "175 ", "1520 ", "1522 ", "1523 ", "1525 ", "162 ",
                      "172 ", "173 ", "174 ", "1570 ", "1573 ", "1575 ", "1577 ", "1578 ", "163 ", "177 ", "178 ",
                      "1590 ", "176 ", "179 ", "1516 ", "180 "]
    #lists with prefixes used, when no country code is included
    prefixes_4_digits = ["0911", "0320", "0700", "0800", "0900", "0160", "0170", "0171", "0175", "0162",
                         "0172", "0173", "0174", "0163", "0177", "0178", "0176", "0179", "0180"]
    prefixes_5_digits = ["09131", "09135", "09133", "01511", "01512", "01514", "01515", "01517", "01520", "01522",
                         "01523", "01525", "01570", "01573", "01575", "01577", "01578", "01590", "01516", "09128"]
    # lists with prefixes used, when country code is included
    prefixes_international_3_digits = ["911", "951", "320", "700", "800", "900", "160", "170", "171", "175", "162",
                                       "172", "173", "174", "163", "177", "178", "176", "179", "180"]
    prefixes_international_4_digits = ["9131", "9135", "9133", "9132" "1511", "1512", "1514", "1515", "1517", "1520",
                                       "1522", "1523", "1525", "1570", "1573", "1575", "1577", "1578", "1590", "1516",
                                       "9128"]
    if phone_number:
        #if no space is between prefix and phone number:
        if not any(code in phone_number for code in prefixes_fixed):
            #remove all blanks
            phone_number = phone_number.replace(" ", "")
            #insert space after country code
            if phone_number[0] == "+":
                phone_number = phone_number[:3] + " " + phone_number[3:]
                #insert space after prefix
                if any(code in phone_number for code in prefixes_international_3_digits):
                    phone_number = phone_number[:7] + " " + phone_number[7:]
                if any(code in phone_number for code in prefixes_international_4_digits):
                    phone_number = phone_number[:8] + " " + phone_number[8:]
            # insert space after prefix
            if any(code in phone_number for code in prefixes_4_digits):
                phone_number = phone_number[:4] + " " + phone_number[4:]
            elif any(code in phone_number for code in prefixes_5_digits):
                phone_number = phone_number[:5] + " " + phone_number[5:]
            #special case : convert 0045 to +49
            if "0049" in phone_number:
                phone_number = phone_number.replace("0049", "+49 ")
            if " -" in phone_number:
                phone_number = phone_number.replace("-", "", 1)

    m1 = phone_numbers_re.search(phone_number)
    if not m1:
        print(phone_number)



audit()