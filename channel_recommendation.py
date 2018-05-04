
# coding: utf-8

import build_matrix as helper
import math
import pandas as pd
from sklearn.metrics import jaccard_similarity_score
import operator
import numpy as np

'''
Item Based Collaborative Filtering Recommender System.

Dataset: Group to Channel subscription.

Recommendation based on Channel - Channel similarity

Similarity Measure used: Jaccard

Author: Pavana Srinivasadeshika Achar

'''

class Recommender(object):

    '''
    Recommender class to compute item-item similarity
    Recommends top K similar items with similarity index

    '''

    def __init__(self, df):
       
        '''
        Constructor: Initialized the dataframe

        '''
        self.df = df
        #self.describe()

    def Jaccard_sim(self, ch1, ch2):
       
        '''
        Uses sklearn Jaccard Similarity Metric
        Takes two values for channels
        Computes Jaccard similarity on the columnar values
        Maximum value : 1 : features are exactly the same for both channels
        
        '''
        return jaccard_similarity_score(self.df[ch1], self.df[ch2])


    def top_similar_channel(self, channel_id, k):
        
        '''
        Function to find top K similar items for a single channel
        Takes values for channel id and K

        '''
        ch = self.df.columns.values
        sims = {}
        for c in ch:
            if not c == channel_id:
                sims[c] = Jaccard_sim(channel_id,c)
        sorted_x = sorted(sims.items(), key=operator.itemgetter(1))
        sorted_x = sorted_x[::-1]
        top_K = sorted_x[0:k]
        return top_K
        

    def subscribed_channels(self, group):

        '''
        Function to separate subscribed and unsubscribed channels
        Takes value for a particular group
        An entry in the utility matrix represents subscription to channel
        A complementary set of the subscribed channels are unsubscribed channels
        
        '''
        print "Retrieving subscribed and unsubscribed channels"
        sub_chans = []
        unsub_chans = []
        ch = self.df.columns.values
        for index, rows in self.df.iterrows():
            if rows['group_id'] == group:
                print "group found..."
                print "classifying channels..."               
                for i in range(2,len(ch)):
                    if int(rows[ch[i]]) == 1:
                        sub_chans.append(ch[i])
                    else:
                        unsub_chans.append(ch[i])
        print "Done"
        return sub_chans, unsub_chans
 



    def recommendation_engine(self, group, k):

        '''
        Function to compute Item based collaborative filtering method
        Takes values for group id and top K
        Unsubscribed channels are possible candidates for recommendation
        Accumulate similarities of unsubscribed with subscribed channels
        Maximum accumulated similarity index used for recommendation
        Returns Top K recommendations for unsubscribed channels
        
        '''
        sub_chans, unsub_chans = self.subscribed_channels(group)
        #print sub_chans, unsub_chans
        sims = {}
        for unsub in unsub_chans:
            for sub in sub_chans:
                sim = self.Jaccard_sim(unsub, sub)
                if unsub in sims.keys():
                    sims[unsub] += sim
                else:
                    sims[unsub] = sim
                      
        print "Fetching top recommendations", 
        sorted_sims = sorted(sims.items(), key=operator.itemgetter(1), reverse = True)
        #top_k_sims = sorted_sims[::-1]
        return sorted_sims[0:k]

    def describe(self):
        print self.df.head()
        return


