from configparser import ConfigParser
import requests
import datetime
import time
import pymysql
import logging


def logger_init():
    logger = logging.getLogger('etl')
    logger.setLevel(logging.INFO)
    logger_sh = logging.StreamHandler()
    logger_fh = logging.FileHandler('etl.log')
    logger_fh.setLevel(logging.ERROR)
    logger_formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s', '%Y-%m-%d %H:%M:%S')
    logger_sh.setFormatter(logger_formatter)
    logger_fh.setFormatter(logger_formatter)
    logger.addHandler(logger_sh)
    logger.addHandler(logger_fh)

    return logger


class ETL_Config():

    def __init__(self, inifile):
        self.file = inifile
        self.config = ConfigParser()
        self.config.read(self.file)

    def __len__(self):
        return len(self.config.sections())

    def __str__(self):
        return 'ETL_Config object (%s)' % ','.join([section for section in self.config.sections()])

    def get_request_url(self, id):
        url = {
            'url': self.config['API_URL']['BaseUrl'],
            'version': self.config['API_URL']['Version'],
            'id': id,
            'resource': self.config['API_URL']['Resource']
        }
        return '{url}/v{version}/{id}/{resource}'.format(**url)

    def get_request_params(self):
        return {
            'fields': self.config['API_URL_Params']['fields'],
            'access_token': self.config['API_URL_Params']['access_token']
        }

    def get_auth_url(self):
        url = {
            'url': self.config['API_URL']['BaseUrl'],
            'resource': self.config['API_URL']['Access']
        }
        return '{url}/{resource}'.format(**url)

    def get_auth_params(self):
        return {
            'client_ID': self.config['API_URL_Params']['client_ID'],
            'client_secret': self.config['API_URL_Params']['client_secret'],
            'grant_type': self.config['API_URL_Params']['grant_type'],
            'fb_exchange_token': self.config['API_URL_Params']['access_token']
        }

    def get_ids(self):
        result = []
        if self.config:
            try:
                ids = self.config['API_URL']['IDs']
                result = [id.strip() for id in ids.split(',')]
            except KeyError as e:
                logger.error('ELT_Config object key error: %s' % e.args)
        return result

    def set_token(self, token):
        self.config['API_URL_Params']['access_token'] = token
        self.config.write(self.file)

    def get_section(self, section_name):
        return {item[0]: item[1] for item in self.config.items(section_name)}


def extract(config, id, url=None, retry_times=2, delay=5):

    def get_request(url, params=None, retry_times=0, delay=0):
        attempts = 0
        while attempts <= retry_times:
            try:
                if params:
                    request = requests.get(url, params=params)
                else:
                    request = requests.get(url)
                break
            except requests.ConnectionError:
                attempts += 1
                request = None
                logger.error('Connection error (request)')
                logger.info('Retry request (%d)' % attempts)
                time.sleep(delay)
            except requests.HTTPError:
                attempts += 1
                request = None
                logger.error('Request is failed (%s)' % str(requests.codes))
                logger.info('Retry request (%d)' % attempts)
                time.sleep(delay)
            except requests.Timeout:
                attempts += 1
                request = None
                logger.error('Timeout error (request)')
                logger.info('Retry request (%d)' % attempts)
                time.sleep(delay)

        return request

    prev_access_token = config.config['API_URL_Params']['access_token']
    attempts = 0
    while attempts <= 1:
        if url:
            logger.info('Getting api request, next page, id=%s' % id)
            request = get_request(url,
                                  retry_times=retry_times,
                                  delay=delay)
        else:
            logger.info('%s api request, start page, id=%s' % ('Getting' if attempts == 0 else 'Retrying to get', id))
            request = get_request(config.get_request_url(id),
                                  params=config.get_request_params(),
                                  retry_times=retry_times,
                                  delay=delay)
        if request:
            if 'error' in request.text:
                info = request.json()
                if info.get('error'):
                    logger.error('API error: {message}\n{type} ({code})'.format(**info.get('error')))
                    if info.get('code') in ('OAuthException', '102', '190'):
                        logger.info('Getting authorization request')
                        request = get_request(config.get_auth_url(), config.get_auth_params())
                        if request:
                            info = request.json()
                            if info.get('error'):
                                logger.error('API error: {message}\n{type} ({code})'.format(**info.get('error')))
                                request = None
                                break
                            else:
                                if info.get('access_token'):
                                    attempts += 1
                                    logger.info('Setting new auth token...')
                                    config.set_token(info.get('access_token'))
                                else:
                                    request = None
                                    break
                        else:
                            request = None
                            break
                    else:
                        request = None
                        break
                else:
                    request = None
                    break
            else:
                break
        else:
            break
    else:
        logger.error('API error: extract procedure with new access token failed!')
        logger.warning('Current access token = {0}, previous access token = {1}'.format(
            config.config['API_URL_Params']['access_token'],
            prev_access_token
        ))

    return request


def transform(request, source, upd_datetime, start_datetime):
    data = []
    next = None

    if not request:
        return data, next

    objects = request.json()
    posts = objects.get('data')
    try:
        next = objects['paging']['next']
    except KeyError:
        pass

    stop_iter = False
    for post in posts:
        if post.get('created_time'):
            date_parts = post.get('created_time').replace('T', '+').split('+')
            post_date = datetime.datetime.strptime(' '.join(date_parts[:-1]), '%Y-%m-%d %H:%M:%S')
            if post_date < start_datetime:
                stop_iter = True
                continue
            else:
                post['created_time'] = post_date
        if post.get('shares'):
            try:
                post['shares'] = post['shares']['count']
            except KeyError:
                post['shares'] = 0
        if post.get('actions'):
            if isinstance(post['actions'], list):
                post['actions'] = len(post['actions'])
        post['source'] = source
        post['update_time'] = upd_datetime
        data.append(post)

    if stop_iter or not data:
        next = None

    return data, next


def extract_transform(config, delay=.400):

    for id in config.get_ids():
        update_time = datetime.datetime.now()
        start_time = update_time + datetime.timedelta(days=-int(config.config['Extract_Params']['LoadDepthInDays']))

        data, next = transform(extract(config, id),
                               id,
                               update_time,
                               start_time)
        yield data

        while next:
            time.sleep(delay)
            data, next = transform(extract(config, id, url=next),
                                   id,
                                   update_time,
                                   start_time)
            yield data


def load(data):
    data_queue.append(data)


def get_db_odjects(config_params):
    try:
        db_conn = pymysql.connect(host=config_params['host'],
                                  port=int(config_params['port']),
                                  user=config_params['user'],
                                  passwd=config_params['passwd'],
                                  db=config_params['db'],
                                  charset=config_params['charset'])
        # db_conn = pymysql.connect(**config_params)
        db_cursor = db_conn.cursor()
    except pymysql.InternalError as e:
        logger.error('DB connection internal error: %s, %s' % (e.args[0], e.args[1]))
    except pymysql.ProgrammingError as e:
        logger.error('DB connection programming error: %s, %s' % (e.args[0], e.args[1]))

    return db_conn, db_cursor


special_symbols = ['\n', '\r' '\t', '\v', '\b']


def clear_value(value):
    value = str(value).replace("'", '_').replace('"', '_')
    for special_symbol in special_symbols:
        value = value.replace(special_symbol, '')
    return value


class DataQueue():

    def __init__(self, config, append_data=list(), size=1000):
        self.config = config
        self.data = []
        self._data_buffer = []
        self.max_size = size
        self.recieved = 0
        self.sent = 0
        self.target = self.config.get_section('Load_Params')
        fields = self.config.config['API_URL_Params']['fields'] + ',source,update_time'
        self.target['fields'] = [field.strip() for field in fields.split(',')]
        self.target['fields_repr'] = ','.join(self.target['fields'])
        self.success_state = True
        self.db_conn, self.db_cursor = get_db_odjects(self.config.get_section('DB_Connection'))
        self.append(append_data)

    def __len__(self):
        return len(self.data)

    def __str__(self):
        return 'Data queue (status {0}): got {1}, sent {2}'.format(
            self.success_state,
            self.recieved,
            self.sent
        )

    def _create_sql_statement(self):
        with open('sql_tmplt.sql') as sql_tmplt:
            sql_template = sql_tmplt.read()

        if not sql_template:
            logger.error('Error ocurred while reading SQL template file!')
            return sql_template

        self.target['values'] = self._get_values()
        sql_template = sql_template.format(**self.target)

        return sql_template

    def _get_values(self):
        values = ''
        row_values = []
        for row in self._data_buffer:
            values += '('
            for field in self.target['fields']:
                if row.get(field):
                    if field in ('created_time', 'update_time'):
                        row_values.append("'" + row[field].strftime('%Y-%m-%d %H:%M:%S') + "'")
                    else:
                        row_values.append("'" + clear_value(row[field]) + "'")
                else:
                    row_values.append('null')
            values += ','.join(row_values) + '),'
            row_values.clear()

        return values[:-1] if values else values

    def _db_cursor_exec(self, sql_statement):
        if not self.success_state:
            return

        try:
            self.db_cursor.execute(sql_statement)
            self.db_conn.commit()
        except pymysql.InternalError as e:
            self.success_state = False
            logger.error('DB connection internal error: %s, %s' % (e.args[0], e.args[1]))
        except pymysql.DataError as e:
            self.success_state = False
            logger.error('DB connection data error: %s, %s' % (e.args[0], e.args[1]))
        except pymysql.OperationalError as e:
            self.success_state = False
            logger.error('DB connection operational error: %s, %s' % (e.args[0], e.args[1]))

        if not self.success_state:
            logger.error('ETL process stopped')
            exit(10)

        self.sent += len(self._data_buffer)
        logger.info('Sent %d, total sent %d' % (len(self._data_buffer), self.sent))

    def append(self, data):
        if data:
            for row in data:
                self.data.append(row)
            self.recieved += len(data)
            logger.info('Recieved %d, total recieved %d' % (len(data), self.recieved))
            self.pop()

    def pop(self, all=False):
        while len(self.data) >= self.max_size or all:
            if not self.data:
                break
            if len(self.data) >= self.max_size:
                self._data_buffer = self.data[:self.max_size]
                del self.data[:self.max_size]
            else:
                self._data_buffer = self.data[:]
                self.data.clear()
            sql = self._create_sql_statement()
            if not sql:
                self.success_state = False

            self._db_cursor_exec(sql)

    def release(self):
        self.pop(all=True)
        logger.info('TOTAL Recieved %d, Sent %d' % (self.recieved, self.sent))
        # if self.db_cursor:
            # self.db_cursor.close()


if __name__ == '__main__':

    logger = logger_init()
    logger.info('ETL process started')

    config = ETL_Config('config.ini')
    logger.info('Base api url: %s' % config.config['API_URL']['BaseUrl'])

    data_queue = DataQueue(config)

    for data in extract_transform(config):
        load(data)

    data_queue.release()

    logger.info('ETL process finished')
