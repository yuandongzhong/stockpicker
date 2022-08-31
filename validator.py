import requests
from lxml import html

class Validator(object):
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'
        }
        self.url = 'https://zhuanlan.zhihu.com/p/53596935'

    def get_token(self):
        try:
            response = requests.get(self.url, headers=self.headers)        
            page = html.fromstring(response.text) 
            title = page.xpath('//h1[@class="Post-Title"]/text()')
            if title:
                token = title[0]
                return token
        except Exception as e:
            print("Validation Error: ", e)