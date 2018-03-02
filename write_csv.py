#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
After auditing is complete the next step is to prepare the data to be inserted into a SQL database.
To do so you will parse the elements in the OSM XML file, transforming them from document format to
tabular format, thus making it possible to write to .csv files.  These csv files can then easily be
imported to a SQL database as tables.

The process for this transformation is as follows:
- Use iterparse to iteratively step through each top level element in the XML
- Shape each element into several data structures using a custom function
- Utilize a schema and validation library to ensure the transformed data is in the correct format
- Write each data structure to the appropriate .csv files

We've already provided the code needed to load the data, perform iterative parsing and write the
output to csv files. Your task is to complete the shape_element function that will transform each
element into the correct format. To make this process easier we've already defined a schema (see
the schema.py file in the last code tab) for the .csv files and the eventual tables. Using the
cerberus library we can validate the output against this schema to ensure it is correct.

## Shape Element Function
The function should take as input an iterparse Element object and return a dictionary.

### If the element top level tag is "node":
The dictionary returned should have the format {"node": .., "node_tags": ...}

The "node" field should hold a dictionary of the following top level node attributes:
- id
- user
- uid
- version
- lat
- lon
- timestamp
- changeset
All other attributes can be ignored

The "node_tags" field should hold a list of dictionaries, one per secondary tag. Secondary tags are
child tags of node which have the tag name/type: "tag". Each dictionary should have the following
fields from the secondary tag attributes:
- id: the top level node id attribute value
- key: the full tag "k" attribute value if no colon is present or the characters after the colon if one is.
- value: the tag "v" attribute value
- type: either the characters before the colon in the tag "k" value or "regular" if a colon
        is not present.

Additionally,

- if the tag "k" value contains problematic characters, the tag should be ignored
- if the tag "k" value contains a ":" the characters before the ":" should be set as the tag type
  and characters after the ":" should be set as the tag key
- if there are additional ":" in the "k" value they and they should be ignored and kept as part of
  the tag key. For example:

  <tag k="addr:street:name" v="Lincoln"/>
  should be turned into
  {'id': 12345, 'key': 'street:name', 'value': 'Lincoln', 'type': 'addr'}

- If a node has no secondary tags then the "node_tags" field should just contain an empty list.

The final return value for a "node" element should look something like:

{'node': {'id': 757860928,
          'user': 'uboot',
          'uid': 26299,
       'version': '2',
          'lat': 41.9747374,
          'lon': -87.6920102,
          'timestamp': '2010-07-22T16:16:51Z',
      'changeset': 5288876},
 'node_tags': [{'id': 757860928,
                'key': 'amenity',
                'value': 'fast_food',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'cuisine',
                'value': 'sausage',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'name',
                'value': "Shelly's Tasty Freeze",
                'type': 'regular'}]}

### If the element top level tag is "way":
The dictionary should have the format {"way": ..., "way_tags": ..., "way_nodes": ...}

The "way" field should hold a dictionary of the following top level way attributes:
- id
-  user
- uid
- version
- timestamp
- changeset

All other attributes can be ignored

The "way_tags" field should again hold a list of dictionaries, following the exact same rules as
for "node_tags".

Additionally, the dictionary should have a field "way_nodes". "way_nodes" should hold a list of
dictionaries, one for each nd child tag.  Each dictionary should have the fields:
- id: the top level element (way) id
- node_id: the ref attribute value of the nd tag
- position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within
            the way element

The final return value for a "way" element should look something like:

{'way': {'id': 209809850,
         'user': 'chicago-buildings',
         'uid': 674454,
         'version': '1',
         'timestamp': '2013-03-13T15:58:04Z',
         'changeset': 15353317},
 'way_nodes': [{'id': 209809850, 'node_id': 2199822281, 'position': 0},
               {'id': 209809850, 'node_id': 2199822390, 'position': 1},
               {'id': 209809850, 'node_id': 2199822392, 'position': 2},
               {'id': 209809850, 'node_id': 2199822369, 'position': 3},
               {'id': 209809850, 'node_id': 2199822370, 'position': 4},
               {'id': 209809850, 'node_id': 2199822284, 'position': 5},
               {'id': 209809850, 'node_id': 2199822281, 'position': 6}],
 'way_tags': [{'id': 209809850,
               'key': 'housenumber',
               'type': 'addr',
               'value': '1412'},
              {'id': 209809850,
               'key': 'street',
               'type': 'addr',
               'value': 'West Lexington St.'},
              {'id': 209809850,
               'key': 'street:name',
               'type': 'addr',
               'value': 'Lexington'},
              {'id': '209809850',
               'key': 'street:prefix',
               'type': 'addr',
               'value': 'West'},
              {'id': 209809850,
               'key': 'street:type',
               'type': 'addr',
               'value': 'Street'},
              {'id': 209809850,
               'key': 'building',
               'type': 'regular',
               'value': 'yes'},
              {'id': 209809850,
               'key': 'levels',
               'type': 'building',
               'value': '1'},
              {'id': 209809850,
               'key': 'building_id',
               'type': 'chicago',
               'value': '366409'}]}
"""

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET

import cerberus

import schema

OSM_PATH = "ex_h59MB33V6XrsLWjzXhs7CWHwY3NCz.osm"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    # YOUR CODE HERE
    if element.tag == 'node':
        node_attribs['id'] = element.attrib['id']
        node_attribs['user'] = element.attrib['user']
        node_attribs['uid'] = element.attrib['uid']
        node_attribs['version'] = element.attrib['version']
        node_attribs['lat'] = element.attrib['lat']
        node_attribs['lon'] = element.attrib['lon']
        node_attribs['timestamp'] = element.attrib['timestamp']
        node_attribs['changeset'] = element.attrib['changeset']
    # print(node_attribs)
    elif element.tag == 'way':
        way_attribs['id'] = element.attrib['id']
        way_attribs['user'] = element.attrib['user']
        way_attribs['uid'] = element.attrib['uid']
        way_attribs['version'] = element.attrib['version']
        way_attribs['timestamp'] = element.attrib['timestamp']
        way_attribs['changeset'] = element.attrib['changeset']

    for tg in element.iter('tag'):
        tag_dict_node = {}
        tag_dict_node['id'] = element.attrib['id']
        if tg.attrib['k'] == 'url':
            tag_dict_node['key'] = clean_url(tg.attrib['v'])
        if tg.attrib['k'] == element.attrib['id']:
            tag_dict_node['key'] = clean_phone_number(tg.attrib['v'])
        else:
            tag_dict_node['key'] = tg.attrib['k']
        if re.search(PROBLEMCHARS, tg.attrib['k']):
            pass
        tag_dict_node['value'] = tg.attrib['v']
        if ':' not in tg.attrib['k']:
            tag_dict_node['type'] = 'regular'
        else:
            tag_type = tg.attrib['k'].split(':')[0]
            tag_dict_node['type'] = tag_type
        # print(tag_dict_node)
        tags.append(tag_dict_node)

    if element.tag == 'node':
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(iter(validator.errors.items()))
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)

        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, str) else v) for k, v in list(row.items())
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

# ================================================== #
#          Data Cleaning Functions                   #
# ================================================== #

def clean_url(url):
    if "www." not in url:
        url = "www." + url
    if "http://" not in url:
        url = "http://" + url
        return url

def clean_phone_number(phone_number):

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
    prefixes_fixed = ["9131 ", "9135 ", "911 ", "9133 ", "320 ", "700 ", "800 ", "900 ", "1511 ", "1512 ", "1514 ",
                      "1515 ", "1517 ", "160 ", "170 ", "171 ", "175 ", "1520 ", "1522 ", "1523 ", "1525 ", "162 ",
                      "172 ", "173 ", "174 ", "1570 ", "1573 ", "1575 ", "1577 ", "1578 ", "163 ", "177 ", "178 ",
                      "1590 ", "176 ", "179 ", "1516 ", "180 "]
    prefixes_4_digits = ["0911", "0320", "0700", "0800", "0900", "0160", "0170", "0171", "0175", "0162", "0172", "0173",
                         "0174", "0163", "0177", "0178", "0176", "0179", "0180"]
    prefixes_5_digits = ["09131", "09135", "09133", "01511", "01512", "01514", "01515", "01517", "01520", "01522",
                         "01523", "01525", "01570", "01573", "01575", "01577", "01578", "01590", "01516", "09128"]
    prefixes_international_3_digits = ["911", "951", "320", "700", "800", "900", "160", "170", "171", "175", "162",
                                       "172", "173", "174", "163", "177", "178", "176", "179", "180"]
    prefixes_international_4_digits = ["9131", "9135", "9133", "9132" "1511", "1512", "1514", "1515", "1517", "1520",
                                       "1522", "1523", "1525", "1570", "1573", "1575", "1577", "1578", "1590", "1516",
                                       "9128"]
    if phone_number:
        if not any(code in phone_number for code in prefixes_fixed):
            #remove all blanks
            phone_number = phone_number.replace(" ", "")
            #insert space after country code
            if phone_number[0] == "+":
                phone_number = phone_number[:3] + " " + phone_number[3:]
                if any(code in phone_number for code in prefixes_international_3_digits):
                    phone_number = phone_number[:7] + " " + phone_number[7:]
                if any(code in phone_number for code in prefixes_international_4_digits):
                    phone_number = phone_number[:8] + " " + phone_number[8:]

            if any(code in phone_number for code in prefixes_4_digits):
                phone_number = phone_number[:4] + " " + phone_number[4:]
            elif any(code in phone_number for code in prefixes_5_digits):
                phone_number = phone_number[:5] + " " + phone_number[5:]
            if "0049" in phone_number:
                phone_number = phone_number.replace("0049", "+49 ")
            if " -" in phone_number:
                phone_number = phone_number.replace("-", "", 1)

        return phone_number


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with open(NODES_PATH, 'w', encoding='utf8') as nodes_file, \
            open(NODE_TAGS_PATH, 'w', encoding='utf8') as nodes_tags_file, \
            open(WAYS_PATH, 'w', encoding='utf8') as ways_file, \
            open(WAY_NODES_PATH, 'w', encoding='utf8') as way_nodes_file, \
            open(WAY_TAGS_PATH, 'w', encoding='utf8') as way_tags_file:

        nodes_writer = csv.DictWriter(nodes_file, NODE_FIELDS, lineterminator='\n')
        node_tags_writer = csv.DictWriter(nodes_tags_file, NODE_TAGS_FIELDS, lineterminator='\n')
        ways_writer = csv.DictWriter(ways_file, WAY_FIELDS, lineterminator='\n')
        way_nodes_writer = csv.DictWriter(way_nodes_file, WAY_NODES_FIELDS, lineterminator='\n')
        way_tags_writer = csv.DictWriter(way_tags_file, WAY_TAGS_FIELDS, lineterminator='\n')

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)

            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSM_PATH, validate=True)
