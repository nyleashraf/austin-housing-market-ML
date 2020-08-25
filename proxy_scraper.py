from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def browse_proxy_list(file_name = 'proxies.txt'):
    f = open(file_name, "w")
    # selenium chrome browser
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    browser = webdriver.Chrome(chrome_options = chrome_options, executable_path = '/Users/nyleashraf/Downloads/chromedriver')
    
    # browse spys.one proxy site
    browser.get("http://spys.one/en/https-ssl-proxy/")
    # browser.get('http://spys.one/proxys/US/')
    browser.execute_script("document.getElementById('xpp').selectedIndex = 5; document.getElementById('xpp').onchange();")
    
    # retrieve HTML elements and remove first element (table header)
    proxy_list = browser.find_elements_by_xpath("//tr[@class='spy1xx' or @class='spy1x']")
    proxy_list.pop(0)
    
    for i in range(len(proxy_list)):
        proxy_server = browser.find_element_by_xpath("//tr[@class='spy1xx' or @class='spy1x'][" + str(i+2) + "]//td[1]//font[@class='spy14']").text 
        f.write(proxy_server)
        f.write('\n')
    
    f.close()

def main():
    browse_proxy_list()
        
if __name__ == "__main__":
    main()