# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class NekretnineUrlsItem(Item):
    add_id = Field()
    add_price = Field()

    # meta podaci
    url = Field()
    project = Field()
    spider = Field()
    server = Field()
    date = Field()


class NekretnineScrapItem(Item):
    # define the fields for your item here like:
    add_id = Field()
    add_json = Field()

    # meta podaci
    url = Field()
    project = Field()
    spider = Field()
    server = Field()
    date = Field()
