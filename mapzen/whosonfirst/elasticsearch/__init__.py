import json
import urllib
import requests
import math
import time
import logging

# https://tenacity.readthedocs.io/en/latest/
    
from tenacity import retry
from tenacity.stop import *
from tenacity.wait import *
from tenacity.after import *

logger = logging.getLogger(__name__)

class base:

    def __init__ (self, **kwargs):

        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 9200)
        self.index = kwargs.get('index', None)
        self.doctype = kwargs.get('doctype', None)

    def endpoint(self):

        url = None

        # because AWS elasticsearch...

        if str(self.port) == '443':
            url = "https://%s" % self.host
        else:
            url = "http://%s:%s" % (self.host, self.port)

        return url

class index (base):

    # https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-index_.html

    def index_document (self, data, **kwargs):

        """
        {
            'id': id,
            'index': self.index,
            'doc_type': doctype,
            'body': body
        }
        """

        url = "%s/%s/%s/%s" % (self.endpoint(), data['index'], data['doc_type'], data['id'])

        body = json.dumps(data['body'])

        try:
            rsp = self.do_index(url, body)
        except Exception, e:
            logging.error("failed to index %s: %s" % (url, e))
            return False

        if not rsp.status_code in (200, 201):
            logging.error("failed to index %s: %s %s" % (url, rsp.status_code, rsp.content))
            return False
            
        return True

    # https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-bulk.html

    def index_documents_bulk (self, iter, **kwargs):

        strict = kwargs.get('strict', True)
        count = kwargs.get('count', 5000)

        logging.info("index bulk documents (%d count) w/strict mode: %s" % (count, strict))
        
        """
        {
            '_id': id,
            '_index': self.index,
            '_type': doctype,
            '_source': body
        }
        """

        url = "%s/_bulk" % (self.endpoint())

        cmds = []

        for index in iter:
            
            # this sucks but we'll live with it for now

            body = index["_source"]
            del(index["_source"])

            index = { 'index': index }

            cmds.append(json.dumps(index))
            cmds.append(json.dumps(body))
            
            # from the docs:
            # NOTE: the final line of data must end with a newline character \n.

            if len(cmds) == count:

                cmds.append("")
                body = "\n".join(cmds)

                try :
                    rsp = self.do_index(url, body)
                except Exception, e:
                    logging.error("failed to index because %s" % e)

                    if strict:
                        return False
                    else:
                        logging.warning("strict-iness is disabled, so chugging along regardless")

                cmds = []

        if len(cmds):

            cmds.append("")
            body = "\n".join(cmds)

            try:            
                headers = { "Content-Type": "application/json" }
                rsp = requests.post(url, data=body, headers=headers)
            except Exception, e:
                logging.error("failed to index %s: %s" % (url, e))

                if strict:
                    return False
                else:
                    logging.warning("strict-iness is disabled, so chugging along regardless")
                        
            if not rsp.status_code in (200, 201):
                logging.error("failed to (bulk) index %s: %s %s" % (url, rsp.status_code, rsp.content))

                if strict:
                    return False
                else:
                    logging.warning("strict-iness is disabled, so chugging along regardless")

        return True

    # see above

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(5), after=after_log(logger, logging.DEBUG))
    def do_index(self, url, body):

        headers = { "Content-Type": "application/json" }
        rsp = requests.post(url, data=body, headers=headers)

        if not rsp.status_code in (200, 201):

            msg = "failed to do_index %s: %s %s" % (url, rsp.status_code, rsp.content)
            logging.error(msg)

            raise Exception, msg

        return rsp

    # https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-delete.html

    def delete_document (self, data):

        """
        {
            'id': id,
            'index': self.index,
            'doc_type': doctype,
            'refresh': True
        }
        """

        url = "%s/%s/%s/%s" % (self.endpoint(), data['index'], data['doc_type'], doc['id'])

        try:
            requests.delete(url)
        except Exception, e:
            logging.error("failed to index %s: %s" % (url, e))
            return False

        return True

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

    def query_count(self, query):

        body = {
            'query': query
        }

        args = {
            'per_page': 1
        }

        rsp = self.query(body=body, params=args)
        rsp = self.standard_rsp(rsp, **args)

        return rsp["pagination"]["total"]
        
    # this depends on all the scroll_id stuff (below in def query(self, **kwargs))

    def query_paginated(self, query, **kwargs) :

        per_page = kwargs.get("per_page", 500)

        body = {
            'query': query
        }

        args = {
            'per_page': per_page,
            'scroll': True
        }

        while True:

            rsp = self.query(body=body, params=args)
            rsp = self.standard_rsp(rsp, **args)

            total = rsp["pagination"]["total"]

            for row in rsp['rows']:
                row = row["_source"]
                yield row

            cursor = rsp['pagination'].get('cursor', "")

            if cursor != '':
                args['scroll_id'] = cursor
            else:
                break

    def query(self, **kwargs) :

        page = self.page
        per_page = self.per_page

        path = kwargs.get('path', '_search')
        body = kwargs.get('body', {})
        params = kwargs.get('params', {})

        es_params = {}

        if params.get('per_page', None):

            per_page = params['per_page']

            if per_page > self.per_page_max:
                per_page = self.per_page_max

        if params.get('page', None):
            page = params['page']

        es_params['from'] = (page - 1) * per_page
        es_params['size'] = per_page

        # scroll stuff is largely - but not exactly - a mirror of this:
        # https://github.com/whosonfirst/whosonfirst-www-api/blob/master/www/include/lib_elasticsearch.php#L15
        # 
        # https://www.elastic.co/guide/en/elasticsearch/guide/current/scroll.html
        #
        # the principal difference is that (for now) you are expected to
        # explicitly pass in params['scroll'] = True to enable scrolling
        # this probably (shouldn't) be the case in the future but unlike
        # the API code there is a bunch of stuff that does traditional
        # page-based pagination and striking out to track every last instance
        # is a bit of yak-shaving exercise right now so it's up to you to
        # invoke scroll-based pagination when it's necessary - for example:
        #
        # body = { 'query': query }
        #
        # args = { 'per_page': 1000, 'scroll': True }
        # cursor = None
        # 
        # while True:
        #
        #     rsp = qry.query(body=body, params=args)
        #     rsp = qry.standard_rsp(rsp, **args)
        # 
        #     cursor = rsp['pagination']['cursor']
        # 
        #     if cursor != '':
        #         args['cursor'] = cursor
        #     else:
        #         break
        #
        # also it goes without saying that we want to implement the standard
        # next_query stuff in the paginate() method below but not today...
        #
        # (20171121/thisisaaronland)
        
        scroll = params.get('scroll', False)
        scroll_id = params.get('scroll_id', None)
        scroll_ttl = params.get('scroll_ttl', '5m')
        scroll_trigger = params.get('scroll_trigger', 10000)        

        if not scroll_id:
            scroll_id = params.get('cursor', None)

        pre_count = False
        
        if scroll and not scroll_id:
            pre_count = True

        if body.has_key("aggregations"):
            scroll = False
            pre_count = False

        # print "DEBUG SCROLL %s SCROLL ID %s PRECOUNT %s" % (scroll, scroll_id, pre_count)

        if pre_count:

            if self.index:
                _url = "%s/%s/%s" % (self.endpoint(), self.index, path)
            else:
                _url = "%s/%s" % (self.endpoint(), path)

            _params = {
                'from': 0,
                'size': 0
            }
            
            _q = urllib.urlencode(_params)
            
            _url = _url + "?" + _q

            _body = json.dumps(body)

            _headers = { "Content-Type": "application/json" }
            
            _rsp = requests.post(_url, data=_body, headers=_headers)
            _data = json.loads(_rsp.content)

	    _hits = _data["hits"];
	    _count = _hits["total"];
            
            if _count < scroll_trigger:
                scroll = False

            # print "DEBUG PRECOUNT _count '%s' trigger '%s' scroll '%s'" % (_count, scroll_trigger, scroll)
            
        #

        body = json.dumps(body)

        t1 = time.time()

        if scroll and scroll_id:

            url = "%s/%s" % (self.endpoint(), path)
            url = url + "/scroll"

            body = {
                'scroll': scroll_ttl,
                'scroll_id': scroll_id,
            }
            
            body = json.dumps(body)

        elif scroll:

            es_params['scroll'] = scroll_ttl

            q = urllib.urlencode(es_params)

            if self.index:
                url = "%s/%s/%s" % (self.endpoint(), self.index, path)
            else:
                url = "%s/%s" % (self.endpoint(), path)

            url = url + "?" + q

        else:

            if self.index:
                url = "%s/%s/%s" % (self.endpoint(), self.index, path)
            else:
                url = "%s/%s" % (self.endpoint(), path)

            if len(es_params.keys()):
                q = urllib.urlencode(es_params)
                url = url + "?" + q

        headers = { "Content-Type": "application/json" }
        rsp = requests.post(url, data=body, headers=headers)

        t2 = time.time()
        t = t2 - t1

        if self.slow_queries != None and t > self.slow_queries:

            msg = "%ss %s#%s (%s)" % (t, self.host, self.index, body)

            if self.slow_queries_log:
                self.slow_queries_log.warning(msg)
            else:
                logging.warning(msg)

        body = json.loads(rsp.content)
        body["mz:timing"] = t

        return body

    def single(self, rsp):

        count = len(rsp['hits']['hits'])

        if count == 0:
            return None

        if count > 1:
            logging.warning("invoking single on a result set with %s results" % count)
            return None

        return rsp['hits']['hits'][0]

    def standard_rsp(self, rsp, **kwargs):

        # as in expired cursors
        # WTF {u'status': 404, 'mz:timing': 0.005153179168701172, u'error': {u'failed_shards': [{u'index': None, u'reason': {u'reason': u'No search context found for id [422628]',                
        if rsp.get("status", None) == 404:

            error = 404

            # be defensive about this in case the response from ES changes
            
            try:
                error = rsp["error"]["root_cause"][0]
            except Exception, e:
                logging.warning("Unable to determine root case for 404 error (%s)", rsp["error"])
                
            return {
                'ok': 0,
                'error': error,    
                'rows': [],
                'pagination': {
                    'total': 0,
                    'count': 0,
                    'per_page': 0,
                    'page': 0,
                    'pages': 0

                },
                'timing': rsp.get("mz:timing", None)
            }

        return {
            'ok': 1,
            'rows': self.rows(rsp),
            'pagination': self.paginate(rsp, **kwargs),
            'timing': rsp.get("mz:timing", None)
        }

    def rows(self, rsp):
        try:
            return rsp['hits']['hits']
        except Exception, e:
            print "WTF %s" % rsp
            return []
        
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

        # see notes above in query() about scroll-based pagination and
        # 'next_query' properties in pagination (20171121/thisisaaronland)

        scroll_id = rsp.get("_scroll_id", None)

        if scroll_id:

            cursor = scroll_id
            pagination["cursor"] = cursor

            if count == 0:
                pagination["cursor"] = ""

            if total <= per_page:
                del(pagination["cursor"])
                
        return pagination
        
