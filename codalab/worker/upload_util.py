from contextlib import closing
import urllib.parse
import http.client
import logging
import socket
from io import StringIO


def upload_with_chunked_encoding(
    method,
    base_url,
    headers,
    query_params,
    fileobj,
    need_response=False,
    url="",
    progress_callback=None,
):
    """
        Uploads the fileobj to url using method with headers and query_params,
        if progress_callback is specified, it is called with the
        number of bytes uploaded after each chunk upload is finished
        the optional progress_callback should return a boolean which interrupts the
        download if False and resumes it if True. If i's not specified the download
        runs to completion

        :param method: String. HTTP request method. Eg, PUT, GET, etc.
        :param base_url: String. Base URL of the upload destination.
        :param headers: Dict. HTTP headers to be contained when uploading.
        :param query_params: Dict. HTTP query parameters to be contained when uploading.
        :param fileobj: File-like Object. The fileobject to be uploaded.
        :param url: String. Location or sub url that indicating where the file object will be uploaded.
        :param need_response: Bool. Whether need to wait for the response.
        :param progress_callback: Function. Callback function indicating upload progress.
        """
    CHUNK_SIZE = 16 * 1024
    TIMEOUT = 60
    # Start the request.
    parsed_base_url = urllib.parse.urlparse(base_url)

    if len(query_params) != 0:
        path = url + '?' + urllib.parse.urlencode(query_params)
    else:
        path = url
    if parsed_base_url.scheme == 'http':
        conn = http.client.HTTPConnection(parsed_base_url.netloc, timeout=TIMEOUT)
    else:
        conn = http.client.HTTPSConnection(parsed_base_url.netloc, timeout=TIMEOUT)

    with closing(conn):
        conn.putrequest(method, base_url + path)  # use full url here

        # Set headers.
        base_headers = {
            'Transfer-Encoding': 'chunked',
        }
        base_headers.update(headers)
        for header_name, header_value in base_headers.items():
            conn.putheader(header_name, header_value)
        conn.endheaders()

        # Use chunked transfer encoding to send the data through.
        bytes_uploaded = 0
        while True:
            to_send = fileobj.read(CHUNK_SIZE)
            if not to_send:
                break
            conn.send(b'%X\r\n%s\r\n' % (len(to_send), to_send))
            bytes_uploaded += len(to_send)
            if progress_callback is not None:
                should_resume = progress_callback(bytes_uploaded)
                if not should_resume:
                    raise Exception('Upload aborted by client')
        conn.send(b'0\r\n\r\n')

        if not need_response:
            return

        # Read the response.
        logging.debug("About to read the response... url: %s", url)

        # Sometimes, it may take a while for the server to process
        # the data and send the response. In this case, we want to
        # periodically keep sending empty bytes so that the
        # connection doesn't drop before the response is available.
        got_response = False
        while not got_response:
            try:
                response = conn.getresponse()
                got_response = True
            except socket.timeout:
                logging.debug("Socket timeout, retrying url: %s", url)
                conn.send(b'\0')
        logging.debug("Finished reading the response, url: %s", url)
        if response.status != 200:
            # Low-level httplib module doesn't throw HTTPError
            raise urllib.error.HTTPError(
                base_url + path,
                response.status,
                response.reason,
                dict(response.getheaders()),
                StringIO(response.read().decode()),
            )
