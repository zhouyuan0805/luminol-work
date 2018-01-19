import urllib2
from bs4 import BeautifulSoup
import sys


reload(sys)
sys.setdefaultencoding('gbk')

url = 'http://news.baidu.com/'
content = urllib2.urlopen(url).read()
soup = BeautifulSoup(content, from_encoding = 'gbk')
hotNews = soup.find_all('div', {'class', 'hotnews'})[0].find_all('li')
for i in hotNews:
    print i.a.text
    print i.a['href']