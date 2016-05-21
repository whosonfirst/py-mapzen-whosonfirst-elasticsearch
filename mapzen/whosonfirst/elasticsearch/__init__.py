# https://pythonhosted.org/setuptools/setuptools.html#namespace-packages
__import__('pkg_resources').declare_namespace(__name__)

import os.path
import json
import logging
import math

import urllib
import requests

# https://elasticsearch-py.readthedocs.org/en/master/

import elasticsearch
import elasticsearch.helpers

# wuh...
# https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html

# return stuff that can be passed to:
# https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/index.html

class base:

    def __init__(self, **kwargs):

        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port', 9200)
        timeout = kwargs.get('timeout', 600)

        port = int(port)
        timeout = float(timeout)

        es = elasticsearch.Elasticsearch(host=host, port=port, timeout=timeout)
        self.es = es

        self.host = host
        self.port = port

        self.index = kwargs.get('index', None)
        self.doctype = kwargs.get('doctype', None)

    def refresh(self):

        self.es.indices.delete(index=self.index, ignore=[400, 404])
        self.es.indices.create(index=self.index)

    def document_id(self, doc):
        raise Exception, "You must define your own 'document_id' method"

    def document_index(self):
        return self.index
    
    def document_type(self, doc):
        return self.doctype
    
class index(base):
    
    def prepare_document(self, doc):

        id = self.document_id(doc)
        body = self.prepare_body(doc)
        doctype = self.document_type(doc)
        
        return {
            'id': id,
            'index': self.index,
            'doc_type': doctype,
            'body': body
        }
    
    # https://stackoverflow.com/questions/20288770/how-to-use-bulk-api-to-store-the-keywords-in-es-by-using-python

    def prepare_document_bulk(self, doc):

        id = self.document_id(doc)
        body = self.prepare_body(doc)        
        doctype = self.document_type(doc)

        return {
            '_id': id,
            '_index': self.index,
            '_type': doctype,
            '_source': body
        }

    def prepare_body(self, doc):

        raise Exception, "You need to define your own prepare_body method"

    def load_file(self, f):

        try:
            fh = open(f, 'r')
            return json.load(fh)
        except Exception, e:
            logging.error("failed to open %s, because %s" % (f, e))
            raise Exception, e

    def prepare_file(self, f):

        doc = self.load_file(f)
        return self.prepare_document(doc)

    def prepare_file_bulk(self, f):

        logging.debug("prepare file %s" % f)

        doc = self.load_file(f)

        doc = self.prepare_document_bulk(doc)
        logging.debug("yield %s" % doc)

        return doc

    def prepare_files_bulk(self, files):

        for path in files:

            logging.debug("prepare file %s" % path)

            data = self.prepare_file_bulk(path)
            logging.debug("yield %s" % data)

            yield data

    def index_file(self, path):

        path = os.path.abspath(path)
        data = self.prepare_file(path)

        return self.es.index(**data)

    def index_files(self, files):

        iter = self.prepare_files_bulk(files)
        return elasticsearch.helpers.bulk(self.es, iter)

    def index_filelist(self, path):

        def mk_files(fh):
            for ln in fh.readlines():
                yield ln.strip()

        fh = open(path, 'r')
        files = mk_files(fh)

        iter = self.prepare_files_bulk(files)
        return elasticsearch.helpers.bulk(self.es, iter)
        
    def delete_document(self, doc):

        id = self.document_id(doc)
        doctype = self.document_type(doc)

        kwargs = {
            'id': id,
            'index': self.index,
            'doc_type': doctype,
            'refresh': True
        }

        self.es.delete(**kwargs)

class query(base):

    def __init__(self, **kwargs):

        base.__init__(self, **kwargs)

        self.page = kwargs.get('page', 1)
        self.per_page = kwargs.get('per_page', 20)

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

    # because who knows what elasticsearch-py is doing half the time...
    # (20150805/thisisaaronland)

    def search_raw(self, **args):

        path = args.get('path', '_search')
        body = args.get('body', {})
        query = args.get('query', {})

        url = "http://%s:%s/%s/%s" % (self.host, self.port, self.index, path)

        if len(query.keys()):
            q = urllib.urlencode(query)
            url = url + "?" + q

        body = json.dumps(body)

        rsp = requests.post(url, data=body)
        return json.loads(rsp.content)

    # https://elasticsearch-py.readthedocs.org/en/master/api.html?highlight=search#elasticsearch.Elasticsearch.search 

    def search(self, body, **kwargs):

        per_page = kwargs.get('per_page', self.per_page)
        page = kwargs.get('page', self.page)
        
        offset = (page - 1) * per_page
        limit = per_page
        
        params = {
            'index': self.index,
            'body': body,
            'from_': offset,
            'size': limit,
        }
        
        if kwargs.get('doctype', None):
            params['doc_type'] = kwargs['doctype']

        rsp = self.es.search(**params)
        hits = rsp['hits']
        total = hits['total']
        
        docs = hits['hits']
            
        pagination = self.paginate(rsp, **kwargs)

        return {'rows': docs, 'pagination': pagination}

    def paginate(self, rsp, **kwargs):

        per_page = kwargs.get('per_page', self.per_page)
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
