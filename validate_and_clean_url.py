import xml.etree.cElementTree as ET
import validators

osm_file = open("ex_h59MB33V6XrsLWjzXhs7CWHwY3NCz.osm", "r", encoding="utf8")


def audit_url(url):
    if not validators.url(url):
        better_url = clean_url(url)
        print(better_url)
        print(validators.url(better_url))


def is_url(elem):
    return (elem.tag == "tag") and (elem.attrib["k"] == "url")


def audit():

    for event, elem in ET.iterparse(osm_file):
        if is_url(elem):
            audit_url(elem.attrib["v"])


def clean_url(url):
    if "www." not in url:
        url = "www." + url
    if "http://" not in url:
        url = "http://" + url
        return url


audit()