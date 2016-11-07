import json
import urllib
import requests
import math
import time
import logging

class base:

    def __init__ (self, **kwargs):

        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 9200)
        self.index = kwargs.get('index', None)

    def __str__ (self):
        return "%s:%s (%s)" % (self.host, self.port, self.index)

class search (base):

    def __init__ (self, **kwargs):

        base.__init__(self, **kwargs)

        self.per_page = kwargs.get('per_page', 100)
        self.per_page_max = kwargs.get('per_page_max', 500)

        self.slow_queries = kwargs.get('slow_queries', None)
        
        if self.slow_queries != None:
            self.slow_queries = float(self.slow_queries)
        
        self.slow_queries_log = kwargs.get('slow_queries_log', None)

        self.page = 1

    def escape(self, str):

        # If you need to use any of the characters which function as operators in
        # your query itself (and not as operators), then you should escape them
        # with a leading backslash. For instance, to search for (1+1)=2, you would
        # need to write your query as \(1\+1\)\=2.
        #
        # The reserved characters are: + - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /
        #
        # Failing to escape these special characters correctly could lead to a
        # syntax error which prevents your query from running.
        #
        # A space may also be a reserved character. For instance, if you have a
        # synonym list which converts "wi fi" to "wifi", a query_string search for
        # "wi fi" would fail. The query string parser would interpret your query
        # as a search for "wi OR fi", while the token stored in your index is
        # actually "wifi". Escaping the space will protect it from being touched
        # by the query string parser: "wi\ fi"
        #
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html

        # note the absence of "&" and "|" which are handled separately

        to_escape = [
            "+", "-", "=", ">", "<", "!", "(", ")", "{", "}", "[", "]", "^", '"', "~", "*", "?", ":", "\\", "/"
        ]

        escaped = []

        unistr = str.decode("utf-8")
        length = len(unistr)

        i = 0

        while i < length:

            char = unistr[i]

            if char in to_escape:
                char = "\%s" % char

            elif char in ("&", "|"):

                if (i + 1) < length and unistr[ i + 1 ] == char:
                    char = "\%s" % char
            else:
                pass

            escaped.append(char)
            i += 1

        return "".join(escaped)

    def query(self, **kwargs) :

        path = kwargs.get('path', '_search')
        body = kwargs.get('body', {})
        params = kwargs.get('params', {})

        if self.index:
            url = "http://%s:%s/%s/%s" % (self.host, self.port, self.index, path)
        else:
            url = "http://%s:%s/%s" % (self.host, self.port, path)

        page = self.page
        per_page = self.per_page

        if params.get('per_page', None):

            per_page = params['per_page']
            del(params['per_page'])

            if per_page > self.per_page_max:
                per_page = self.per_page_max

        if params.get('page', None):
            page = params['page']
            del(params['page'])

        params['_from'] = (page - 1) * per_page
        params['size'] = per_page

        if len(params.keys()):
            q = urllib.urlencode(params)
            url = url + "?" + q
            
        body = json.dumps(body)

        t1 = time.time()

        rsp = requests.post(url, data=body)

        t2 = time.time()
        t = t2 - t1

        if self.slow_queries != None and t > self.slow_queries:

            msg = "%ss %s#%s (%s)" % (t, self.host, self.index, body)

            if self.slow_queries_log:
                self.slow_queries_log.warning(msg)
            else:
                logging.warning(msg)

        return json.loads(rsp.content)

    def single(self, rsp):

        count = len(rsp['hits']['hits'])

        if count == 0:
            return None

        if count > 1:
            logging.warning("invoking single on a result set with %s results" % count)
            return None

        return rsp['hits']['hits'][0]

    def standard_rsp(self, rsp):

        return {
            'ok': 1,
            'rows': self.rows(rsp),
            'pagination': self.paginate(rsp)
        }

    def rows(self, rsp):
        return rsp['hits']['hits']

    def paginate(self, rsp, **kwargs):

        per_page = kwargs.get('per_page', self.per_page)

        if per_page > self.per_page_max:
            per_page = self.per_page_max

        page = kwargs.get('page', self.page)

        hits = rsp['hits']
        total = hits['total']

        docs = hits['hits']
        count = len(docs)

        pages = float(total) / float(per_page)
        pages = math.ceil(pages)
        pages = int(pages)

        pagination = {
            'total': total,
            'count': count,
            'per_page': per_page,
            'page': page,
            'pages': pages
        }

        return pagination
        
