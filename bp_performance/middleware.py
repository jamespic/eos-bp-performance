import datetime

from werkzeug.wrappers import Request, Response

def cache_middleware(expiry_seconds):
    expiry = datetime.timedelta(seconds=expiry_seconds)
    cache = {}
    def wrapper(f):
        def wrapped(environ, start_response):
            req = Request(environ)
            path = req.full_path
            cached = cache.get(path)
            if cached and not (
                    req.cache_control.no_store or req.cache_control.no_cache):
                updated_time, content, status, headers = cached
                if updated_time + expiry > datetime.datetime.now():
                    # Cached version still valid
                    browser_cache_valid = False
                    etag = str(hash(content))
                    if req.if_none_match.contains(etag):
                        browser_cache_valid = True
                    if req.if_modified_since:
                        if updated_time < req.if_modified_since:
                            browser_cache_valid = True
                    if browser_cache_valid:
                        start_response('304 Not Modified', [])
                        return []
                    else:
                        response = Response(content, status, headers)
                        response.add_etag(etag)
                        response.date = datetime.datetime.now()
                        return response(environ, start_response)
            # Cached version not valid
            sent_status = None
            sent_headers = []
            def inner_start_response(status, headers, exc=None):
                nonlocal sent_status, sent_headers
                if exc:
                    start_response(status, headers, exc)
                else:
                    sent_status = status
                    sent_headers = list(headers)
            response_iter = f(environ, inner_start_response)
            if sent_status:
                # Success! Cache and send response
                content = tuple(response_iter)
                cache[path] = (datetime.datetime.now(), content, sent_status, sent_headers)
                response = Response(content, sent_status, sent_headers)
                response.set_etag(str(hash(content)))
                response.date = datetime.datetime.now()
                return response(environ, start_response)
            else:
                # Don't cache the exception handler, just propagate
                return response_iter
        return wrapped
    return wrapper
