import re
import json
import random
import requests
import logging
import time
import numpy as np
import pandas as pd
import argparse
from concurrent.futures import ProcessPoolExecutor
from bs4 import BeautifulSoup
import sqlite3
import fake_useragent
from itertools import cycle

from filters import apply_filters

def create_tables_if_not_exist():
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS URLS
             (
             URL            TEXT    NOT NULL,
             NUM_PROPERTIES INT,
             NUM_PAGES      INT,
             PER_PAGE_PROPERTIES   INT);''')
    conn.execute('''CREATE TABLE IF NOT EXISTS LISTINGS
             (
             URL            TEXT    NOT NULL,
             INFO           TEXT);''')
    conn.execute('''CREATE TABLE IF NOT EXISTS LISTING_DETAILS
             (
             URL            TEXT    NOT NULL,
             NUMBER_OF_ROOMS INT,
             NAME           TEXT,
             COUNTRY        TEXT,
             REGION         TEXT,
             LOCALITY       TEXT,
             STREET         TEXT,
             POSTOAL        TEXT,
             TYPE           TEXT,
             PRICE          REAL
             );''')
    conn.close()

def construct_proxy(ip_addr, port):

    return {
    'http': f'http://{ip_addr}:{port}',
    'https': f'https://{ip_addr}:{port}',
    }

def time_proxy(ip_addr, port, proxy_user=None, proxy_pass=None,  url='https://www.redfin.com', timeout=10, TOTAL_TRIES_PER_URL=2):
    """Check all proxies in proxy.csv for ability to connect
    to redfin.com. Each proxy requests connection to the site
    twice, and only those with 100% success rate (2/2) kept.
    """
    success_counts = 0
    ua = fake_useragent.UserAgent()

    pull_proxies = construct_proxy(ip_addr, port)

    for i in range(TOTAL_TRIES_PER_URL):
        try:
            r = requests.get(url, proxies=pull_proxies, headers={'User-agent': ua.chrome}, timeout=30)
            if r.status_code == 200:
                success_counts += 1
        except Exception:
            pass

    print('for proxy {}'.format(pull_proxies))
    print('success rate = {}'.format(success_counts / TOTAL_TRIES_PER_URL))
    return success_counts / TOTAL_TRIES_PER_URL

def find_successful_proxies(proxies):
    """Using list of [ip, port] pairs, find proxies that can
    successfully connect to redfin.com.
    Utilizes time_proxy function to request connection to
    redfin.com for each proxy in list.
    """
    successful_proxies = []
    while proxies:
        success_rate = time_proxy(proxies[-1][1], proxies[-1][2])
        if success_rate == 1.0:
            successful_proxies.append(proxies.pop())

    return successful_proxies

def get_page_info(url_and_proxies):
    """
    Return property count, page count and total properties under a given URL.

    :param url_and_proxies: list, refers to single url from Redfin.com with filters applied
        and list of ip:port pairs for proxies scraping from openproxy.space
    :returns: list of total properties, number of pages, and number of properties per page for the url
    """
    url, proxies = url_and_proxies
    # LOGGER.info('Requesting {} url'.format(url))
    print('Requesting {} url'.format(url))
    random.shuffle(proxies)
    proxy_pool = cycle(proxies)

    time.sleep(random.random() * 10)
    session = requests.Session()
    total_properties, num_pages, properties_per_page = None, None, None
    count = 0
    num_proxies = len(proxies)
    while count <= num_proxies:
        try:
            proxy_element = next(proxy_pool)
            proxy = construct_proxy(proxy_element[1], proxy_element[2])
            # LOGGER.info('Using {} proxy'.format(proxy))
            print('Using {} proxy'.format(proxy))
            resp = session.get(url, headers=HEADER, proxies=proxy, timeout=30)
            resp.raise_for_status()
            # LOGGER.info('Got {} status code.'.format(resp.status_code))
            print('Got {} status code.'.format(resp.status_code))

            if resp.status_code == 200:
                bf = BeautifulSoup(resp.text, 'lxml')
                page_description_div = bf.find('div', {'class': 'homes summary'})
                if not page_description_div:
                    # The page has nothing!
                    return(url, 0, 0, 20)
                page_description = page_description_div.get_text()
                if 'of' in page_description:
                    property_cnt_pattern = r'Showing ([0-9]+) of ([0-9]+) .*'
                    m = re.match(property_cnt_pattern, page_description)
                    if m:
                        properties_per_page = int(m.group(1))
                        total_properties = int(m.group(2))
                    pages = [int(x.get_text()) for x in bf.find_all('a', {'class': "goToPage"})]
                    num_pages = max(pages)
                else:
                    property_cnt_pattern = r'Showing ([0-9]+) .*'
                    m = re.match(property_cnt_pattern, page_description)
                    if m:
                        properties_per_page = int(m.group(1))
                    num_pages = 1

                return (url, total_properties, num_pages, properties_per_page)

        except Exception as e:
            # LOGGER.exception('Swallowing exception {} on url {}'.format(e, url))
            print('Swallowing exception {} on url {}'.format(e, url))
            count += 1
            continue
    return (url, total_properties, num_pages, properties_per_page)

def url_partition(base_url, proxies, max_levels=6, LOGGER = None):
    """Partition the listings for a given url into multiple sub-urls,
    such that each url contains at most 20 properties.
    """
    urls = [base_url]
    num_levels = 0
    partitioned_urls = []
    while urls and (num_levels < max_levels):
#         rand_move = random.randint(0, len(proxies) - 1)
        partition_inputs = []
        for url in urls:
            # proxy = construct_proxy(*proxies[(rand_move + i) % len(proxies)][1:3])
            # LOGGER.debug(f"scraping url {url} with proxy {proxy}")
            partition_inputs.append((url, proxies))

        scraper_results = []
        with ProcessPoolExecutor(max_workers=min(50, len(partition_inputs))) as executor:
            # must pass in the two args for get_page_info: url and list of all proxies
            # in order for map function to work as expected, need to pass in list of all urls
            # list takes form [[url1, (proxy1, proxy2, ...)], [url2, (proxy1, proxy2, ...)], ...]
            scraper_results = list(executor.map(get_page_info, partition_inputs))

        # LOGGER.info('Getting {} results'.format(len(scraper_results)))
        # LOGGER.info('Results: {}'.format(scraper_results))
        print('Getting {} results'.format(len(scraper_results)))
        print('Results: {}'.format(scraper_results))

        values = []
        for result in scraper_results:
            to_nulls = [x if x else 'NULL' for x in result]
            values.append("('{}', {}, {}, {})".format(*to_nulls))

        print("Values from search criteria, Step {}:\n {}".format(num_levels + 1, values))


        with sqlite3.connect(SQLITE_DB_PATH) as db:
            print('stage {} saving to db!'.format(num_levels))
            # LOGGER.info('stage {} saving to db!'.format(num_levels))
            values = []
            for result in scraper_results:
                to_nulls = [x if x else 'NULL' for x in result]
                values.append("('{}', {}, {}, {})".format(*to_nulls))
            cursor = db.cursor()
            cursor.execute("""
                INSERT INTO URLS (URL, NUM_PROPERTIES, NUM_PAGES, PER_PAGE_PROPERTIES)
                VALUES {};
            """.format(','.join(values)))

        # LOGGER.info('Writing to value list {} results'.format(len(scraper_results)))
        print("Writing to value list {} results".format(len(scraper_results)))
        new_urls = []
        for result in scraper_results:
            if (result[1] and result[2] and result[3] and result[1] > result[2] * result[3]) or (num_levels == 0):
                expanded_urls = apply_filters(result[0], base_url)
                if len(expanded_urls) == 1 and expanded_urls[0] == result[0]:
                    # LOGGER.info('Cannot further split {}'.format(result[0]))
                    print("Cannot further split {}".format(result[0]))
                else:
                    new_urls.extend(expanded_urls)
            else:
                partitioned_urls.append(result)
        # LOGGER.info('stage {}: running for {} urls. We already captured {} urls'.format(
        #     num_levels, len(new_urls), len(partitioned_urls)))
        print("stage {}: running for {} urls. We already captured {} urls".format(
            num_levels, len(new_urls), len(partitioned_urls)))
        urls = new_urls
        num_levels += 1
        time.sleep(random.randint(2, 5))
    # return partitioned_urls

# -------------------------------------- START WORKING SPACE --------------------------------------

# list returned from url_partition must be passed in as argument since we're not include DB functionality rn 
def get_paginated_urls(prefix):
    # Return a set of paginated urls with at most 20 properties each.
    paginated_urls = []
    with sqlite3.connect(SQLITE_DB_PATH) as db:
        cursor = db.execute("""
            SELECT URL, NUM_PROPERTIES, NUM_PAGES, PER_PAGE_PROPERTIES
            FROM URLS
        """)
        seen_urls = set()
        for row in cursor:
            url, num_properties, num_pages, per_page_properties = row
            if prefix and (prefix not in url):
                continue
            if url in seen_urls:
                continue
            if num_properties == 0:
                continue
            urls = []
            # print(num_properties, num_pages, per_page_properties, url)
            if not num_pages:
                urls = [url]
            elif (not num_properties) and int(num_pages) == 1 and per_page_properties:
                urls = ['{},sort=lo-price/page-1'.format(url)]
            elif num_properties < num_pages * per_page_properties:
                # Build per page urls.
                # print('num pages {}'.format(num_pages))
                urls = ['{},sort=lo-price/page-{}'.format(url, p) for p in range(1, num_pages + 1)]
            paginated_urls.extend(urls)
    return list(set(paginated_urls))

# will need to change logic to run through random proxies until successful connection is made 
def scrape_page(url_and_proxies):
    url, proxies = url_and_proxies
    random.shuffle(proxies)
    proxy_pool = cycle(proxies)
    time.sleep(random.random() * 16)
    details = []
    num_proxies = len(proxies)
    count = 0
    while count <= num_proxies:
        try:
            proxy_element = next(proxy_pool)
            proxy = construct_proxy(proxy_element[1], proxy_element[2])
            session = requests.Session()
            resp = session.get(url, headers=HEADER, proxies=proxy, timeout=30)
            resp.raise_for_status()
            print('Got {} status code.'.format(resp.status_code))

            if resp.status_code == 200:
                bf = BeautifulSoup(resp.text, 'lxml')
                details = [json.loads(x.text) for x in bf.find_all('script', type='application/ld+json')]
                return url, json.dumps(details)

        except Exception:
            # LOGGER.exception('failed for url {}, proxy {}'.format(url, proxy))
            print('failed for url {}, proxy {}'.format(url, proxy))
            count += 1
            continue

# will need to change logic to run through random proxies until successful connection is made 
def crawl_redfin_with_proxies(proxies, prefix=''):
    small_urls = get_paginated_urls(prefix)
    # rand_move = random.randint(0, len(proxies) - 1)
    scrape_inputs, scraper_results = [], []
    for url in small_urls:
        # proxy = construct_proxy(*proxies[(rand_move + i) % len(proxies)])
        scrape_inputs.append((url, proxies))

    with ProcessPoolExecutor(max_workers=min(50, len(scrape_inputs))) as executor:
        scraper_results = list(executor.map(scrape_page, scrape_inputs))

    # LOGGER.warning('Finished scraping!')
    print('Finished scraping!')

    # return scraper_results

    with sqlite3.connect(SQLITE_DB_PATH) as db:
        cursor = db.cursor()
        for result in scraper_results:
            url, info = result
            try:
                cursor.execute("""
                    INSERT INTO LISTINGS (URL, INFO)
                    VALUES (?, ?)""", (url, info))
            except Exception as e:
                # LOGGER.info('failed record: {}'.format(result))
                # LOGGER.info(e)
                print('failed record: {}'.format(result))
                print(e)

def parse_addresses():
    listing_details = {}
    with sqlite3.connect(SQLITE_DB_PATH) as db:
        cur = db.cursor()
        cur.execute("SELECT * FROM listings")
        rows = cur.fetchall()
        urls = set()

        for url, json_details in rows:
            if url in urls:
                continue
            urls.add(url)
            listings_on_page = (json.loads(json_details))
            for listing in listings_on_page:
                # print('listing {}'.format(listing))
                num_rooms, name, country, region, locality, street, postal, house_type, price = \
                    None, None, None, None, None, None, None, None, None
                listing_url = None
                if (not isinstance(listing, list)) and (not isinstance(listing, dict)):
                    continue

                if isinstance(listing, dict):
                    info = listing
                    if ('url' in info) and ('address' in info):
                        listing_url = info.get('url')
                        address_details = info['address']
                        num_rooms = info.get('numberOfRooms')
                        name = info.get('name')
                        country = address_details.get('addressCountry')
                        region = address_details.get('addressRegion')
                        locality = address_details.get('addressLocality')
                        street = address_details.get('streetAddress')
                        postal = address_details.get('postalCode')
                        house_type = info.get('@type')
                        listing_details[listing_url] = (listing_url, num_rooms, name, country,
                                                        region, locality, street, postal, house_type, price)
                    continue

                for info in listing:
                    if ('url' in info) and ('address' in info):
                        listing_url = info.get('url')
                        address_details = info['address']
                        num_rooms = info.get('numberOfRooms')
                        name = info.get('name')
                        country = address_details.get('addressCountry')
                        region = address_details.get('addressRegion')
                        locality = address_details.get('addressLocality')
                        street = address_details.get('streetAddress')
                        postal = address_details.get('postalCode')
                        house_type = info.get('@type')
                    if 'offers' in info:
                        price = info['offers'].get('price')
                if listing_url:
                    listing_details[listing_url] = (listing_url, num_rooms, name, country,
                                                    region, locality, street, postal, house_type, price)

    # print(listing_details)
    with sqlite3.connect(SQLITE_DB_PATH) as db:
        cursor = db.cursor()
        try:
            cursor.executemany("""
                INSERT INTO LISTING_DETAILS (
                             URL,
                             NUMBER_OF_ROOMS,
                             NAME     ,
                             COUNTRY  ,
                             REGION    ,
                             LOCALITY  ,
                             STREET    ,
                             POSTOAL   ,
                             TYPE      ,
                             PRICE)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, listing_details.values())
        except Exception as e:
            # LOGGER.info(e)
            print(e)

# -------------------------------------- END WORKING SPACE --------------------------------------

if __name__ == '__main__':
    # base_url = 'https://www.redfin.com/city/1362/CA/Belmont/filter/include=sold-3yr'
    base_url = 'https://www.redfin.com/city/30818/TX/Austin/filter/include=sold-3yr'

    SQLITE_DB_PATH = 'redfin-scraper-data.db'

    LOGGER = None

    HEADER = {
        'User-agent': 'Chrome'
    }

    create_tables_if_not_exist()
    
    proxy_csv_path = 'proxy.csv'
    proxies = pd.read_csv(proxy_csv_path, encoding='utf-8').values
    proxies = proxies.tolist()

    url_partition(base_url, proxies)
    crawl_redfin_with_proxies(proxies)
    parse_addresses()
    # urls_df = pd.DataFrame(urls, columns = ['URL', 'NUM_PROPERTIES', 'NUM_PAGES', 'PER_PAGE_PROPERTIES'])
    # print(urls_df)
