import pandas as pd

proxies = open('proxies.txt')
list_of_proxies = []

for proxy in proxies:
    ip_port = proxy.rsplit(' ')[-1]
    ip_port = ip_port.rsplit(':')
    ip_address = ip_port[0]
    # port = ip_port[1].rsplit('>')[0]
    port = ip_port[1].rsplit('\n')[0]
    ip_and_port = [ip_address, port]
    list_of_proxies.append(ip_and_port)

proxies.close()
df = pd.DataFrame(list_of_proxies)
df.to_csv('proxy.csv', header=['ip', 'port'])