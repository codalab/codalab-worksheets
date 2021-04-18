import urllib

from unittest.mock import MagicMock
from urllib.response import addinfourl

urlopen_real = urllib.request.urlopen


class MockNetwork:
    """Sets up a way to mock network requests for unit tests. Test classes that want to
    use this functionality should just inherit from MockNetwork. Then, the function
    mock_url_sources() can be used to construct a URL that returns the specified
    file contents.
    """

    def setUp(self):
        urllib.request.urlopen = urlopen_real
        super().setUp()

    def mock_url_sources(self, fileobj, ext=""):
        """Returns a URL that is mocked to return the contents of fileobj.
        The URL will end in the extension "ext", if given.
        """
        url = f"https://codalab/contents{ext}"
        size = len(fileobj.read())
        fileobj.seek(0)
        urllib.request.urlopen = MagicMock()
        urllib.request.urlopen.return_value = addinfourl(fileobj, {"content-length": size}, url)
        return [url]
