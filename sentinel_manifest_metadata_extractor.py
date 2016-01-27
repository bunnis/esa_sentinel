'''Python script for extracting metadata fields from sentinel manifests
Sentinel 1
Sentinel 2
Assumes there is a location where several manifests are stored
January 2016'''
from lxml import etree
import os

class SentinelMetadataExtractor:
  filepath = ''
  metadataTree = []
  currentTree = ""
  root = ""
  file_error_count=0
  filenames_error=[]
  
  def __init__(self, Filepath="/tmp/harvested/manifests/"):
    self.filepath = Filepath;
    
    
  def extractMetadata(self):
    for i in os.listdir(self.filepath):
      if i.endswith(".safe") or i.endswith(".SAFE"): 
          print "processing file - "+str(i)
          
          self.tree = etree.parse(self.filepath+"/"+str(i))
          self.root = self.tree.getroot()
          
          
          raw = ['S1A_S3_RAW__0SDH','S1A_IW_RAW__0SSV','S1A_IW_RAW__0SSH','S1A_IW_RAW__0SDV','S1A_IW_RAW__0SDH','S1A_EW_RAW__0SDH']
          gr = ['S1A_S3_GRDH_1SDH','S1A_IW_SLC__1SSV','S1A_IW__1SSH','S1A_IW_SLC__1SDV','S1A_IW_SLC__1SDH','S1A_IW_GRDH_1SSV','S1A_IW_GRDH_1SSH',
                'S1A_IW_GRDH_1SDV','S1A_IW_GRDH_1SDH','S1A_EW_GRDM_1SSH','S1A_EW_GRDM_1SDV','S1A_EW_GRDH_1SDH','S1A_IW_GRDH_1SDV','S1A_EW_GRDM_1SDH']
          
          processed = False
          for sentinel_name in gr:
            if i.startswith(sentinel_name):
              processed = True
              self.extractGR()
              break
            
          for sentinel_name in raw:
            if i.startswith(sentinel_name):
              processed = True
              self.extractRAW()
              break
            
          if i.startswith('S1A_IW_OCN__2SDV'):
              processed = True
              self.extractIWOCN()
              break
            
          elif not processed:
              self.file_error_count = self.file_error_count+1
              self.filenames_error.append(str(i))
              print "FILE NOT IN KNOW FILES - "+str(i)
      else:
        print "Current file not ending with .SAFE or .safe - "+str(i)

    
  def extractGR(self):
    metadata = {}
    ###############S1A_S3_GRDH_1SDH###############S1A_IW_SLC__1SSV###############S1A_IW__1SSH###############S1A_IW_SLC__1SDV
    ###############S1A_IW_SLC__1SDH###############S1A_IW_GRDH_1SSV###############S1A_IW_GRDH_1SSH###############S1A_IW_GRDH_1SDV
    ###############S1A_IW_GRDH_1SDH###############S1A_EW_GRDM_1SSH###############S1A_EW_GRDM_1SDV###############S1A_EW_GRDH_1SDH
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:startTime',self.root.nsmap)
    metadata['startTime'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:stopTime',self.root.nsmap)
    metadata['stopTime'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:familyName',self.root.nsmap)
    metadata['familyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:number',self.root.nsmap)
    metadata['familyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:familyName',self.root.nsmap)
    metadata['instrumentFamilyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:extension/s1sarl1:instrumentMode/s1sarl1:mode',self.root.nsmap)
    metadata['instrumentMode'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productClass',self.root.nsmap)
    metadata['productClass'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productClassDescription',self.root.nsmap)
    metadata['productClassDescription'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productComposition',self.root.nsmap)
    metadata['productComposition'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productType',self.root.nsmap)
    metadata['productType'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:transmitterReceiverPolarisation',self.root.nsmap)
    if len(extracted) > 1:
      metadata['transmitterReceiverPolarisation'] = extracted[0].text + "/" + extracted[1].text
    else:
      metadata['transmitterReceiverPolarisation'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:frameSet/safe:frame/safe:footPrint/gml:coordinates',self.root.nsmap)
    metadata['coordinates'] = extracted[0].text
    #
    return metadata
    
  def extractRAW(self):
    metadata = {}
    ###############S1A_S3_RAW__0SDH###############S1A_IW_RAW__0SSV###############S1A_IW_RAW__0SSH###############S1A_IW_RAW__0SDV
    ###############S1A_IW_RAW__0SDH###############S1A_EW_RAW__0SDH
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}acquisitionPeriod/{http://www.esa.int/safe/sentinel-1.0}startTime',self.root.nsmap)
    metadata['startTime'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}acquisitionPeriod/{http://www.esa.int/safe/sentinel-1.0}stopTime',self.root.nsmap)
    metadata['stopTime'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}familyName',self.root.nsmap)
    metadata['familyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}number',self.root.nsmap)
    metadata['familyNameNumber'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}instrument/{http://www.esa.int/safe/sentinel-1.0}familyName',self.root.nsmap)
    metadata['instrumentFamilyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}instrument/{http://www.esa.int/safe/sentinel-1.0}extension/s1sar:instrumentMode/s1sar:mode',self.root.nsmap)
    metadata['instrumentMode'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:productClass',self.root.nsmap)
    metadata['productClass'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:productClassDescription',self.root.nsmap)
    metadata['productClassDescription'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:productConsolidation',self.root.nsmap)
    metadata['productConsolidation'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:transmitterReceiverPolarisation',self.root.nsmap)
    if len(extracted) > 1:
      metadata['transmitterReceiverPolarisation'] = extracted[0].text + "/" + extracted[1].text
    else:
      metadata['transmitterReceiverPolarisation'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}frameSet/{http://www.esa.int/safe/sentinel-1.0}frame/{http://www.esa.int/safe/sentinel-1.0}footPrint/{http://www.opengis.net/gml}coordinates',self.root.nsmap)
    metadata['coordinates'] = extracted[0].text
    
    return metadata
  
  def extractIWOCN(self):
    metadata = {}
    ###############S1A_IW_OCN__2SDV
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:startTime',self.root.nsmap)
    metadata['startTime'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:stopTime',self.root.nsmap)
    metadata['stopTime'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:familyName',self.root.nsmap)
    metadata['familyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:number',self.root.nsmap)
    metadata['familyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:familyName',self.root.nsmap)
    metadata['instrumentFamilyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:extension/s1sarl2:instrumentMode/s1sarl2:mode',self.root.nsmap)
    metadata['instrumentMode'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productClass',self.root.nsmap)
    metadata['productClass'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productClassDescription',self.root.nsmap)
    metadata['productClassDescription'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productComposition',self.root.nsmap)
    metadata['productComposition'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productType',self.root.nsmap)
    metadata['productType'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:transmitterReceiverPolarisation',self.root.nsmap)
    if len(extracted) > 1:
      metadata['transmitterReceiverPolarisation'] = extracted[0].text + "/" + extracted[1].text
    else:
      metadata['transmitterReceiverPolarisation'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:frameSet/safe:frame/safe:footPrint/gml:coordinates',self.root.nsmap)
    metadata['coordinates'] = extracted[0].text
    
    return metadata
  
  def checkAllFilesParsed(self):
    print '\n\n'
    print "printing nr of errors and filenames"
    print self.file_error_count
    for i in range(0,len(self.filenames_error)):
      print self.filenames_error[i]
    
    
    
sentinel = SentinelMetadataExtractor()
sentinel.extractMetadata()
sentinel.checkAllFilesParsed()
