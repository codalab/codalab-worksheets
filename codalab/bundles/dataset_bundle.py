'''
DatasetBundle is a Bundle type that simply inherits from UploadedBundle.
'''
from codalab.bundles.uploaded_bundle import UploadedBundle


class DatasetBundle(UploadedBundle):
    BUNDLE_TYPE = 'dataset'
