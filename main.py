from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import NotConfigured

from nekretnine_scrap.spiders.halo_nekretnine import HaloUrlsSpider, HaloSpider
import psycopg2
from nekretnine_scrap.pipelines.pg import PostgresWriter

configure_logging()
runner = CrawlerRunner(settings=get_project_settings())


@defer.inlineCallbacks
def crawl():
    # prvo skidam sve oglase koje cu scrapovati
    yield runner.crawl(HaloUrlsSpider)
    # sada citam sve urls koje je pokupio prvi spider
    pg_url = get_project_settings().get('PG_PIPELINE_URL', None)
    if not pg_url:
        raise NotConfigured
    conn_kwargs = PostgresWriter.parse_pg_url(pg_url)
    conn = psycopg2.connect(**conn_kwargs)
    cur = conn.cursor()
    cur.execute("""SELECT urls_to_dl.url FROM urls_to_dl
    LEFT JOIN staging ON
    urls_to_dl.add_id = staging.add_id
    WHERE staging.add_id IS NULL;""")
    res = cur.fetchall()
    HaloSpider.URLS = [item for t in res for item in t]
    cur.close()
    conn.close()
    # na kraju pokrecem drugi spider koji skida sve sto treba
    yield runner.crawl(HaloSpider)
    reactor.stop()

crawl()
reactor.run()
# https://stackoverflow.com/questions/25170682/running-scrapy-from-script-not-including-pipeline

# Ovako sam ranije pokretao zbog debug moda u pycharm
# from scrapy import cmdline
# cmdline.execute("scrapy crawl halo_nekretnine".split())
