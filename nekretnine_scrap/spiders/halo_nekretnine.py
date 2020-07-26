import datetime as dt
import json
import scrapy
import re
import socket

from scrapy.loader import ItemLoader
from nekretnine_scrap.items import NekretnineScrapItem, NekretnineUrlsItem


class HaloUrlsSpider(scrapy.Spider):
    name = 'get_urls_halo_nekretnine'
    custom_settings = {
        'ITEM_PIPELINES': {
            'nekretnine_scrap.pipelines.pg.GetUrlsPGWriter': 400
        }
    }

    def start_requests(self):
        urls = ["https://www.halooglasi.com/nekretnine/prodaja-stanova/beograd",
                "https://www.halooglasi.com/nekretnine/izdavanje-stanova/beograd",
                "https://www.halooglasi.com/nekretnine/prodaja-kuca/beograd",
                "https://www.halooglasi.com/nekretnine/izdavanje-kuca/beograd",
                "https://www.halooglasi.com/nekretnine/prodaja-garaza/beograd",
                "https://www.halooglasi.com/nekretnine/izdavanje-garaza/beograd"
                ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, meta={'base_url':url})

    def parse(self, response):
        raw_json = re.search(r'(?<=QuidditaEnvironment.serverListData=).*?(?=\};)', response.text).group(0)
        raw_json = raw_json + '}'
        data = json.loads(raw_json)
        total_pages = data['TotalPages']
        base_url = response.meta.get('base_url')
        pages = [f"{base_url}?page={p}" for p in range(1, total_pages + 1)]

        for page in pages:
            # next_page = response.urljoin(page)
            # self.logger.info(f'{next_page}')
            yield scrapy.Request(page, callback=self.parse_page)

    def parse_page(self, response):
        for add in response.css(".my-product-placeholder"):
            l = ItemLoader(item=NekretnineUrlsItem())
            l.add_value('add_id', add.css("::attr(data-id)").extract_first())
            l.add_value('add_price', add.css("::attr(data-value)").extract_first())
            url_raw = add.css("::attr(href)").extract_first()
            url = response.urljoin(url_raw)
            l.add_value('url', url)
            l.add_value('project', self.settings.get('BOT_NAME'))
            l.add_value('spider', self.name)
            l.add_value('server', socket.gethostname())
            l.add_value('date', dt.datetime.now())
            yield l.load_item()


class HaloSpider(scrapy.Spider):
    name = 'halo_nekretnine'
    URLS = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'nekretnine_scrap.pipelines.pg.GetAdsPGWriter': 4
        }
    }

    def start_requests(self):
        for url in self.URLS:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        l = ItemLoader(item=NekretnineScrapItem(), response=response)
        # cistim od line endinga
        raw_json = re.search(r'(?<=QuidditaEnvironment.CurrentClassified=).*?(?=\};)', response.text).group(0)
        raw_json = raw_json + '}'
        add_json_clean = json.loads(raw_json)
        add_json = json.dumps(add_json_clean)
        l.add_value('add_id', add_json_clean['Id'])
        l.add_value('add_json', add_json)

        # meta polja
        l.add_value('url', response.url)
        l.add_value('project', self.settings.get('BOT_NAME'))
        l.add_value('spider', self.name)
        l.add_value('server', socket.gethostname())
        l.add_value('date', dt.datetime.now())

        return l.load_item()
