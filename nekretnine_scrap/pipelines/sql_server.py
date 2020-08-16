from twisted.internet import defer
from twisted.enterprise import adbapi
from scrapy.exceptions import NotConfigured
import dj_database_url
import pyodbc
import traceback


class SQLServerWriter:
    """A pipeline that writes to SQL Server database"""

    @classmethod
    def from_crawler(cls, crawler):
        """Retrive scrapy crawler and accesses pipelines settings"""

        # Get URL from settings

        sql_url = crawler.settings.get('SQL_SERVER_PIPELINE_URL', None)

        # Ukoliko ga nema ugasi pipeline
        if not sql_url:
            raise NotConfigured

        return cls(sql_url)

    def __init__(self, sql_url):
        """Otvori pool ka bazi"""

        self.sql_url = sql_url
        # Prijavi  connection error samo jednom
        self.report_connection_error = True

        # inicijalizuj konekciju
        conn_str = SQLServerWriter.parse_sql_url(sql_url)
        print(conn_str)
        self.dbpool = adbapi.ConnectionPool('pyodbc', conn_str, autocomit=True)

    def close_spider(self, spider):
        """Discard db pool kada se zatvori spider"""
        self.dbpool.close()

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        """Ovo insertuje u bazu"""

        logger = spider.logger

        try:
            yield self.dbpool.runInteraction(self.insert_item, item)
        except pyodbc.DatabaseError as e:
            if self.report_connection_error:
                logger.error(f"Cant connect to DB:{self.sql_url} error: {e}")
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
    def parse_sql_url(pg_url):
        """Parsuje url i priprema argumente za adbapi.ConnectionPool()"""

        params = dj_database_url.parse(pg_url)

        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={params['HOST']};DATABASE={params['NAME']};UID={params['USER']};PWD={params['PASSWORD']}"
        return conn_str


class GetUrlsSQLWriter(SQLServerWriter):

    @staticmethod
    def insert_item(tx, item):
        sql = """INSERT INTO urls_to_dl (add_id, add_price, url, project, spider, server, date) 
            VALUES (?,?,?,?,?,?,?)"""

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


class GetAdsSQLWriter(SQLServerWriter):

    @staticmethod
    def insert_item(tx, item):
        """Ovo izvrsava SQL"""

        sql = """INSERT INTO staging (add_id, add_json, url, project, spider, server, date) 
            VALUES (?,?,?,?,?,?,?)"""

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

