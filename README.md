# ESA Sentinel Products Metadata Extractor written in Python
Program written in python to parse xml product metadata from ESA Sentinel Products. Supports Sentinel 1 and Sentinel 2.


```
from sentinel_manifest_metadata_extractor import SentinelMetadataExtractor

location = "/tmp/harvested/manifests/"

sentinel = SentinelMetadataExtractor(location)

#get metadata from manifests files located at 'location'
sentinel.extractMetadataFromManifestFiles()
sentinel.checkAllFilesParsed() #prints a report of errors for files

#API metadata extraction
#if downloadManifests is true, then download to 'location'
DHUS_USER ='someuser'
DHUS_pass='somepass'
sentinel.extractMetadataFromAPIForToday(downloadManifests = False,user=DHUS_USER, password=DHUS_pass,outputFolder = location)


#the actual metadata, dict in which keys are the filenames in lowercase, values are a dict of keys,values
metadata = sentinel.getProductsMetadata() 

for productName in metadata.keys(): #generate inspires for all metadata harvested
    #write to manifests-inspire, based on the template, a xml inspire metadata document
    sentinel.generateInspireFromTemplate(productName,'inspire_template.xml','/tmp/harvested/manifests-inspire/', True) 
```
