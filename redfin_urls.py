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

from filters import apply_filters

base_url = 'https://www.redfin.com/city/1362/CA/Belmont/filter/include=sold-3yr'
# base_url = 'https://www.redfin.com/city/30818/TX/Austin/filter/include=sold-3yr'

LOGGER = None

HEADER = {
    'User-agent': 'Chrome'
}

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
    # start = time.time()
    ua = fake_useragent.UserAgent()

    pull_proxies = construct_proxy(ip_addr, port)
   
    for i in range(TOTAL_TRIES_PER_URL):
        try:
            r = requests.get(url, proxies=pull_proxies, headers={'User-agent': ua.chrome}, timeout=timeout)
            if r.status_code == 200:
                success_counts += 1
        except Exception as e:
            pass
#             print(e)

    print('for proxy {}'.format(pull_proxies))
#     print('total time {} for visiting {} times'.format(time.time() - start, TOTAL_TRIES_PER_URL))
    print('success rate = {}'.format(success_counts / TOTAL_TRIES_PER_URL))
    return success_counts / TOTAL_TRIES_PER_URL

def find_successful_proxies(proxies):
    """Using list of [ip, port] pairs, find proxies that can
    successfully connect to redfin.com. 
    Utilizes time_proxy function to request connection to 
    redfin.com for each proxy in list.
    """
    unsuccessful_proxies = []
    successful_proxies = []
    while proxies:
        success_rate = time_proxy(proxies[-1][1], proxies[-1][2])
        if success_rate == 1.0:
            successful_proxies.append(proxies.pop())
        # if success_rate != 1.0:
        #     unsuccessful_proxies.append(proxies.pop())
        # else:
        #     successful_proxies.append(proxies.pop())
    #     if len(successful_proxies) == 50:
    #         break
    return successful_proxies

def get_page_info(url_and_proxy):
    """Return property count, page count and total properties under a given URL."""
    url, proxy = url_and_proxy
    LOGGER.info('Requesting {} url'.format(url))
    # print("Requesting {} url".format(url))
    LOGGER.info('Using {} proxy'.format(proxy))
    # print('Using {} proxy'.format(proxy))

    time.sleep(random.random() * 10)
    session = requests.Session()
    total_properties, num_pages, properties_per_page = None, None, None
    try:
        resp = session.get(url, headers=HEADER, proxies=proxy)
        resp.raise_for_status()
        LOGGER.info('Got {} status code.'.format(resp.status_code))
        # print('Got {} status code.'.format(resp.status_code))

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
    except Exception as e:
        LOGGER.exception('Swallowing exception {} on url {}'.format(e, url))
        # print('Swallowing exception {} on url {}'.format(e, url))
    return (url, total_properties, num_pages, properties_per_page)

def url_partition(base_url, proxies, max_levels=6):
    """Partition the listings for a given url into multiple sub-urls,
    such that each url contains at most 20 properties.
    """
    urls = [base_url]
    num_levels = 0
    partitioned_urls = []
    while urls and (num_levels < max_levels):
        rand_move = random.randint(0, len(proxies) - 1)
        partition_inputs = []
        for i, url in enumerate(urls):
            proxy = construct_proxy(*proxies[(rand_move + i) % len(proxies)][1:3])
            LOGGER.debug(f"scraping url {url} with proxy {proxy}")
            # print("scraping url {} with proxy {}".format(url, proxy))
            partition_inputs.append((url, proxy))

        scraper_results = []
        with ProcessPoolExecutor(max_workers=min(50, len(partition_inputs))) as executor:
            scraper_results = list(executor.map(get_page_info, partition_inputs))

        LOGGER.info('Getting {} results'.format(len(scraper_results)))
        LOGGER.info('Results: {}'.format(scraper_results))
        # print("Getting {} results".format(len(scraper_results)))
        # print("Results: {}".format(scraper_results))

        values = []
        for result in scraper_results:
            to_nulls = [x if x else 'NULL' for x in result]
            values.append("('{}', {}, {}, {})".format(*to_nulls))
        
        print("Values from search criteria, Step {}:\n {}".format(num_levels + 1, values))
            
        
#         with sqlite3.connect(SQLITE_DB_PATH) as db:
#             LOGGER.info('stage {} saving to db!'.format(num_levels))
#             values = []
#             for result in scraper_results:
#                 to_nulls = [x if x else 'NULL' for x in result]
#                 values.append("('{}', {}, {}, {})".format(*to_nulls))
#             cursor = db.cursor()
#             cursor.execute("""
#                 INSERT INTO URLS (URL, NUM_PROPERTIES, NUM_PAGES, PER_PAGE_PROPERTIES)
#                 VALUES {};
#             """.format(','.join(values)))

        LOGGER.info('Writing to value list {} results'.format(len(scraper_results)))
        # print("Writing to value list {} results".format(len(scraper_results)))
        new_urls = []
        for result in scraper_results:
            if (result[1] and result[2] and result[3] and result[1] > result[2] * result[3]) or (num_levels == 0):
                expanded_urls = apply_filters(result[0], base_url)
                if len(expanded_urls) == 1 and expanded_urls[0] == result[0]:
                    LOGGER.info('Cannot further split {}'.format(result[0]))
                    # print("Cannot further split {}".format(result[0]))
                else:
                    new_urls.extend(expanded_urls)
            else:
                partitioned_urls.append(result)
        LOGGER.info('stage {}: running for {} urls. We already captured {} urls'.format(
            num_levels, len(new_urls), len(partitioned_urls)))
        # print("stage {}: running for {} urls. We already captured {} urls".format(
        # num_levels, len(new_urls), len(partitioned_urls)))
        urls = new_urls
        num_levels += 1
        time.sleep(random.randint(2, 5))
    return partitioned_urls

if __name__ == '__main__':
    # base_url = 'https://www.redfin.com/city/1362/CA/Belmont/filter/include=sold-3yr'
    base_url = 'https://www.redfin.com/city/30818/TX/Austin/filter/include=sold-3yr'

    LOGGER = None

    HEADER = {
        'User-agent': 'Chrome'
    }

    proxy_csv_path = 'proxy.csv'
    proxies = pd.read_csv(proxy_csv_path, encoding='utf-8').values
    proxies = proxies.tolist()
    successful_proxies = find_successful_proxies(proxies)

    urls = url_partition(base_url, successful_proxies)
    urls_df = pd.DataFrame(urls, columns = ['URL', 'NUM_PROPERTIES', 'NUM_PAGES', 'PER_PAGE_PROPERTIES'])
    print(urls_df)
