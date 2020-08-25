from bs4 import BeautifulSoup
import requests, lxml.html


def get_home_info(url):

    # CODE TO CHECK FOR HIDDEN INPUTS ON REDFIN LOGIN PAGE

    # s = requests.Session()
    # login_url = 'https://www.redfin.com/login-v2'
    # login = s.get(login_url)
    # login_html = lxml.html.fromstring(login.text)
    # hidden_inputs = login_html.xpath(r'//form//input[@type="hidden"]')
    # form = {x.attrib["name"]: x.attrib["value"] for x in hidden_inputs}
    # print(form)

    HEADER = {
        'User-agent': 'Chrome'
    }

    login_url = 'https://www.redfin.com/login-v2'

    payload = {
        'emailInput': 'nashraf19@utexas.edu',
        'passwordInput': 'Scraper1'
    }

    session = requests.Session()
    resp = session.post(login_url, headers=HEADER, data=payload)
    print(resp.url)
    
    # resp = session.get(login_url, headers=HEADER, timeout=30)
    # resp.raise_for_status()
    # print('Got {} status code.'.format(resp.status_code))

    # if resp.status_code == 200:
    #     bf = BeautifulSoup(resp.text, 'lxml')
    #     redfin_price_description_div = bf.find('div', {'class': 'info-block avm'})
    #     redfin_estimate = redfin_price_description_div.find('div', {'class': 'statsValue'}).text
    #     print(redfin_estimate)
    #     actual_price_description_div = bf.find('div', {'class': 'info-block price'})
    #     actual_price = actual_price_description_div.find('div', {'class': 'statsValue'}).text
    #     print(actual_price)
    #     # beds_div = bf.find('div', {'class': 'info-block', 'data-rf-test-id': 'abp-beds'})
    #     # num_beds = beds_div.find('div', {'class': 'statsValue'}).text
    #     # print(num_beds)
    #     # baths_div = bf.find('div', {'class': 'info-block', 'data-rf-test-id': 'abp-baths'})
    #     # num_baths = beds_div.find('div', {'class': 'statsValue'}).text
    #     # print(num_baths)

if __name__ == '__main__':
    get_home_info('https://www.redfin.com/TX/Austin/2502-Royal-Lytham-Dr-78747/home/31840544')

