from twisted.internet import defer
from twisted.enterprise import adbapi
from scrapy.exceptions import NotConfigured
import dj_database_url
import psycopg2
import traceback


class PostgresWriter:
    """A pipeline that writes to Postgresql database"""

    @classmethod
    def from_crawler(cls, crawler):
        """Retrive scrapy crawler and accesses pipleines settings"""

        # Get Postgres URL from settings

        pg_url = crawler.settings.get('PG_PIPELINE_URL', None)

        # Ukoliko ga nema ugasi pipeline
        if not pg_url:
            raise NotConfigured

        return cls(pg_url)

    def __init__(self, pg_url):
        """Otvori pool ka PG bazi"""

        self.pg_url = pg_url
        # Prijavi  connection error samo jednom
        self.report_connection_error = True

        # inicijalizuj konekciju
        conn_kwargs = PostgresWriter.parse_pg_url(pg_url)
        print(conn_kwargs)
        self.dbpool = adbapi.ConnectionPool('psycopg2',
                                            # charset='utf-8',
                                            # use_unicode=True,
                                            # connect_timeout=5,
                                            **conn_kwargs)

    def close_spider(self, spider):
        """Discard db pool kada se zatvori spider"""
        self.dbpool.close()

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        """Ovo insertuje u bazu"""

        logger = spider.logger

        try:
            yield self.dbpool.runInteraction(self.insert_item, item)
        except psycopg2.DatabaseError as e:
            if self.report_connection_error:
                logger.error(f"Cant connect to DB:{self.pg_url} error: {e}")
                self.report_connection_error = False
        except:
            print(traceback.format_exc())

        defer.returnValue(item)

    @staticmethod
    def insert_item(tx, item):
        """Ovde ide SQL koji je potrebno izvrsiti
        tx: connection
        item: item to insert"""
        pass

    @staticmethod
    def parse_pg_url(pg_url):
        """Parsuje PG url i pripremi argumente za adbapi.ConnectionPool()"""

        params = dj_database_url.parse(pg_url)

        conn_kwargs = {'host': params['HOST'],
                       'user': params['USER'],
                       'password': params['PASSWORD'],
                       'dbname': params['NAME'],
                       'port': params['PORT']}

        # Remove items with empty values
        conn_kwargs = dict((k, v) for k, v in conn_kwargs.items() if v)

        return conn_kwargs


class GetUrlsPGWriter(PostgresWriter):

    @staticmethod
    def insert_item(tx, item):
        sql = """INSERT INTO urls_to_dl (add_id, add_price, url, project, spider, server, date) 
            VALUES (%s,%s,%s,%s,%s,%s,%s)"""

        args = (
            item['add_id'][0],
            item['add_price'][0],
            item['url'][0],
            item['project'][0],
            item['spider'][0],
            item['server'][0],
            item['date'][0]
        )

        tx.execute(sql, args)


class GetAdsPGWriter(PostgresWriter):

    @staticmethod
    def insert_item(tx, item):
        """Ovo izvrsava SQL"""

        sql = """INSERT INTO staging (add_id, add_json, url, project, spider, server, date) 
            VALUES (%s,%s,%s,%s,%s,%s,%s)"""

        args = (
            item['add_id'][0],
            item['add_json'][0],
            item['url'][0],
            item['project'][0],
            item['spider'][0],
            item['server'][0],
            item['date'][0]
        )

        tx.execute(sql, args)
