from bs4 import BeautifulSoup
import requests, lxml.html
import sqlite3
import re


def create_table_if_not_exists(SQLITE_DB_PATH):
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS LISTING_DETAILS
            (
            ADDRESS                   TEXT    NOT NULL,
            LOCALITY                  TEXT    NOT NULL,
            REGION                    TEXT    NOT NULL,
            POSTAL CODE               TEXT    NOT NULL,
            PRICE                     TEXT    NOT NULL,
            NUMBER_OF_BEDS            INT,
            NUMBER_OF_BATHS           INT,
            WALK_SCORE                INT,
            TRANSIT_SCORE             INT,
            BIKE_SCORE                INT,
            SCHOOL1_TITLE             TEXT,
            SCHOOL1_DISTANCE          TEXT,
            SCHOOL1_RATING            INT,
            SCHOOL2_TITLE             TEXT,
            SCHOOL2_DISTANCE          TEXT,
            SCHOOL2_RATING            INT,
            SCHOOL3_TITLE             TEXT,
            SCHOOL3_DISTANCE          TEXT,
            SCHOOL3_RATING            INT,
            NUMBER_OF_DINING_ROOMS    INT,
            NUMBER_OF_LIVING_ROOMS    INT,
            NUMBER_OF_OTHER_ROOMS     INT,
            DINING_ROOM_DESCRIPTION   TEXT,
            KITCHEN_FEATURES          TEXT,
            KITCHEN_APPLIANCES        TEXT,
            SCHOOL DISTRICT           TEXT,
            NUMBER_OF_PARKING_SPACES  INT,
            PARKING_FEATURES          TEXT,
            YEAR_BUILT                TEXT,
            NUMBER_OF_FIREPLACES      INT,
            HOA                       TEXT,
            HOA_DUES                  TEXT,
            POOL                      TEXT,
            POOL_FEATURES             TEXT,
            NUMBER_OF_STORIES         INT,
            AREA_AMENITIES            TEXT
            );''')
    conn.close()

def get_home_info(url_and_proxies):

    url, proxies = url_and_proxies
    random.shuffle(proxies)
    proxy_pool = cycle(proxies)
    time.sleep(random.random() * 16)
    num_proxies = len(proxies)
    count = 0

    home_features = ['# of Beds', '# of Baths', '# of Dining Rooms', '# of Living Rooms',\
                    'Other Rooms', 'Dining Room Description', 'Kitchen Features', \
                    'Kitchen Appliances', 'School District', '# of Parking Spaces', \
                    'Parking Features', 'Year Built', '# of Fireplaces', 'Has HOA', \
                    'HOA Dues', 'Has Pool', 'Pool Features', '# of Stories', 'Area Amenities']

    HEADER = {
        'User-agent': 'Chrome'
    }

    while count <= len(proxies):
        try:
            proxy_element = next(proxy_pool)
            proxy = construct_proxy(proxy_element[1], proxy_element[2])
            session = requests.Session()
            resp = session.get(url, headers=HEADER, timeout=30)
            resp.raise_for_status()
            print('Got {} status code.'.format(resp.status_code))

            if resp.status_code == 200:
                bf = BeautifulSoup(resp.text, 'lxml')
                # pull basic home information
                address_div = bf.find('h1', {'class': 'address inline-block'})
                address = address_div.find('span', {'class': 'street-address'}).text 
                locality = address_div.find('span', {'class': 'locality'}).text 
                region = address_div.find('span', {'class': 'region'}).text 
                postal = address_div.find('span', {'class': 'postal-code'}).text 
                redfin_price_description_div = bf.find('div', {'class': 'info-block price'})
                redfin_estimate = redfin_price_description_div.find('div', {'class': 'statsValue'}).text
                beds_div = bf.find('div', {'class': 'info-block', 'data-rf-test-id': 'abp-beds'})
                num_beds = beds_div.find('div', {'class': 'statsValue'}).text
                baths_div = bf.find('div', {'class': 'info-block', 'data-rf-test-id': 'abp-baths'})
                num_baths = baths_div.find('div', {'class': 'statsValue'}).text
                running_list = [address, locality, region, postal, redfin_estimate, num_beds, num_baths]
                # find transportation scores for home
                walking_div = bf.find('div', {'class': 'transport-icon-and-percentage walkscore'})
                walkscore = walking_div.find('span', {'class': re.compile('value*')}).text
                running_list.append(walkscore)
                transit_div = bf.find('div', {'class': 'transport-icon-and-percentage transitscore'})
                transitscore = transit_div.find('span', {'class': re.compile('value*')}).text
                running_list.append(transitscore)
                biking_div = bf.find('div', {'class': 'transport-icon-and-percentage bikescore'})
                bikescore = biking_div.find('span', {'class': re.compile('value*')}).text
                running_list.append(bikescore)
                # find nearby school data for home
                for element in bf.find_all('tr', {'class': 'schools-table-row'}):
                    school_title = element.find('div', {'class': 'school-title'}).text
                    school_distance = element.find('div', {'class': 'value'}).text
                    school_rating = element.find('span', {'class': 'rating-num'}).text
                    running_list.extend((school_title, school_distance, school_rating))
                # pull data from the listing details container
                for feature in home_features:
                    appended = False
                    for element in bf.find_all('span', {'class': 'entryItemContent'}):
                        if ':' in element.text:
                            if feature in element.text.split(':')[0]:
                                running_list.append(element.text.split(':')[1].strip())
                                appended = True
                        else:
                            if feature in element.text:
                                running_list.append(element.text)
                                appended = True
                    if appended == False:
                        running_list.append('NULL')
                return running_list
        except Exception:
            print('failed for url {}, proxy {}'.format(url, proxy))
            count += 1
            continue


def link_checker(link = 'https://www.redfin.com/city/30818/TX/Austin/filter/include=sold-3mo/page-2'):

    HEADER = {
        'User-agent': 'Chrome'
    }

    url_list = []

    session = requests.Session()
    resp = session.get(link, headers=HEADER, timeout=30)
    resp.raise_for_status()
    print('Got {} status code.'.format(resp.status_code))

    if resp.status_code == 200:
        bf = BeautifulSoup(resp.text, 'lxml')
        for link in bf.find_all('a'):
            if type(link.get('href')) is str and link.get('href').startswith('/TX/Austin/'):
                url_list.append(link.get('href'))
    print(list(set(url_list)))


if __name__ == '__main__':
    # get_home_info('https://www.redfin.com/TX/Austin/6736-Blarwood-Dr-78745/home/40629546')
    link_checker()

