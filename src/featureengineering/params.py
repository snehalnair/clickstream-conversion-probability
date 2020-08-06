MIN_VISITS = 1
VOCABSIZE_ARRAYCOLS = 40
PAD_LEN = 3



VAR_TYPES = {
  'FEATURE_VARS': [
    'fullVisitorId',
    'visitStartTime',
    'channelGrouping',
    'trafficSource.medium',
    'device.deviceCategory',
    'trafficSource.source',
    'geoNetwork.city',
    'totals.timeOnSite',
    'pageTitle',
    'hits.page.pagePathLevel2',
    'hits.page.pagePathLevel3',
    'hits.contentGroup.contentGroup4',
    'visitsIn0',
    'transactionsIn0'
  ],
  
  
  'PARTITION_COL' : 'fullVisitorId',
  'ORDERBY_SORT_COL': 'visitStartTime',
  
  'STRING_COLS': [
    'channelGrouping',
    'medium',
    'deviceCategory',
    'source',
    'city'
 ],
  

  'NUM_COLS' : [
    'visitStartTime',
    'timeOnSite',
    'transactions',
    'visitNumber',
    'visitsIn0',
    'transactionsIn0',
    'timeBetweenVisits',
    'visitsIn0Future',
    'transactionsIn0Future'
  ],

  'X_NUM_COLS' : [
    'timeOnSite',
    'timeBetweenVisits',
  ],
  
  'ARRAY_COLS' : [
    'pagePathLevel1',
    'pagePathLevel2',
    'pagePathLevel3',
    'contentGroup4',
],

  'Y_COLS': [
    'visitsIn0',
    'transactionsIn0'
],
  
 'FINAL_COLS':[
#  'fullVisitorId',
#  'visitStartTime',
 'timeOnSite_scaled',
 'timeBetweenVisits_scaled',
 'channelGrouping_index',
 'medium_index',
 'deviceCategory_index',
 'source_index',
 'city_index',
 'pagePathLevel1_vector',
 'pagePathLevel2_vector',
 'pagePathLevel3_vector',
 'contentGroup4_vector',
 'visitsIn0_binary',
 'transactionsIn0_binary',
#  'embedding_features',
#  'scale_features',
#  'convolution_features'
              ],
  
  
 'EMBEDDING_COLS':[
 'channelGrouping_index',
 'medium_index',
 'deviceCategory_index',
 'source_index',
 'city_index',
 ],
  
 'CONV_COLS':[
 'pagePathLevel1_vector',
 'pagePathLevel2_vector',
 'pagePathLevel3_vector',
 'contentGroup4_vector'],
  
 'SCALE_COLS':[
 'timeOnSite_scaled',
 'timeBetweenVisits_scaled'],
  
 'LABELS_VISIT':[
 'visitsIn0_binary'],

 'LABELS_TRANS':[
 'transactionsIn0_binary']

}