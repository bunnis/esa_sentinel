# ESA Sentinel Products Metadata Extractor written in Python
Program written in python to parse xml product metadata from ESA Sentinel Products. Supports Sentinel 1 and Sentinel 2.


```
from sentinel_manifest_metadata_extractor import SentinelMetadataExtractor

sentinel = SentinelMetadataExtractor("/tmp/harvested/manifests/")
sentinel.extractMetadata()
sentinel.checkAllFilesParsed() #prints a report of errors

metadata = sentinel.getProductsMetadata() #the actual metadata, dict in which keys are the filenames in lowercase, values are a dict of keys,values

for productName in metadata.keys(): #generate inspires for all metadata harvested
    sentinel.generateInspireFromTemplate(productName,'inspire_template.xml','/tmp/harvested/manifests-inspire/') #write to manifests-inspire, based on the template, a xml inspire metadata document
```
