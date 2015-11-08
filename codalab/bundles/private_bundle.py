'''
DatasetBundle is a Bundle type that simply inherits from UploadedBundle.
'''
from codalab.objects.bundle import Bundle

class PrivateBundle(Bundle):
    """
    Dummy placeholder subclass of Bundle that is used to mask private bundles.
    """
    BUNDLE_TYPE = 'private'
    METADATA_SPECS = ()
