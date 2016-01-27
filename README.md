# ESA Sentinel Products Metadata Extractor written in Python
Program written in python to parse xml product metadata from ESA Sentinel Products. Supports Sentinel 1 and Sentinel 2.


--
from sentinel_manifest_metadata_extractor import SentinelMetadataExtractor

sentinel = SentinelMetadataExtractor("/tmp/harvested/manifests/")
sentinel.extractMetadata()

metadata = sentinel.getProductsMetadata() #metadata is a dict with key beeing the name of the product and the value a dict of keys and the corresponding value
