import pandas as pd
import numpy as np

def function(data):

    '''
    This function maps the values None : 0 and Entries : 1
    '''

    if data == None:
        return 0
    else:
        return 1


def build_utility_matrix(source_path, target_path):
    '''
    The source file is just group-to-channel relational data
    This may have redundant rows for group ids and column ids
    Objective is to build a utility matrix where each column and row are unique
    ''' 
    print "Loading the Training dataset"
    train_df = pd.read_csv(source_path, names=['group_id','channel_id'])
    print "Done"

    '''
    One Hot encoding is a method of representing categorical features uniformly
    across all the rows
    '''
    print "Computing OneHotEncoding"
    pdf = train_df.pivot(index='group_id', columns='channel_id', values='channel_id')
    print "Done"

    '''
    The pivot function fills the empty cells with the value None.
    Since we are using the utility matrix to compute similarities,
    the entries should be numerical.
    The similarity metric is Jaccard Coefficient.
    Therefore, we need the entries to be binary.
    '''

    print "Mapping Nones to 0 and values to 1"
    for c in pdf.columns.values:
        pdf[c] = map(function, pdf[c])

    print "Done"

    '''
    presist the utility matrix at the target location
    '''

    print "Writing to file"
    pdf.to_csv(target_path)
    print "Done"
    return

def load_data(path):
    train_df = pd.read_csv(path, names=['group_id','channel_id'])
    return train_df
