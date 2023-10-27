from codalab.worker.download_util import get_target_info, BundleTarget

bundle_uuid = "0x00b700acad894081afb1982bb70441b3"
bundle_location = f"../bundles_test/{bundle_uuid}"
target = BundleTarget(bundle_uuid, subpath='')
info = get_target_info(bundle_location, target, depth=0)
print(info)
