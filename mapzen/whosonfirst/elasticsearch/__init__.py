# https://pythonhosted.org/setuptools/setuptools.html#namespace-packages
__import__('pkg_resources').declare_namespace(__name__)

import json
import urllib
import requests
import math

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

        self.page = 1

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

        rsp = requests.post(url, data=body)
        return json.loads(rsp.content)

    def single(self, rsp):

        count = len(rsp['hits']['hits'])

        if count == 0:
            return None

        if count > 1:
            logging.warning("invoking single on a result set with %s results" % count)
            return None

        return rsp['hits']['hits'][0]

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
        
