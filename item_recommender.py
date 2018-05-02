import build_matrix as bm
import pandas as pd
from channel_recommendation import Recommender as reco
import matplotlib.pyplot as plt
import sys

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Please specify the group_id you want recommendation for.')
    elif len(sys.argv) < 3:
        print('Please specify the number of recommendations you want.')
    else:
        try:
            group = str(sys.argv[1])
            k = int(sys.argv[2])
        except:
            print "Invalid number for recommendations."
    print "reading data"
    train = pd.read_csv('store/utility_matrix.csv')
    print"done"

    re = reco(train)

    print "Getting top ", k," Recommendation for group: ", group
    recs = re.recommendation_engine(group, k)
    with open("item_recommendations_for_"+str(group)+".csv","w+") as f:
        f.write('channel_id, score\n')
        for i in recs:
            f.write("%s,%s\n"%(str(i[0]),str(i[1])))
    print "Recommendations saved. Location: item_recommendations_for_",str(group),".csv"
