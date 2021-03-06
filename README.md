# social-graph-analytics

### Instructions
#### 1. Install pyspark
  - Windows: https://medium.com/@GalarnykMichael/install-spark-on-windows-pyspark-4498a5d8d66c
  - Linux: https://medium.com/@GalarnykMichael/install-spark-on-ubuntu-pyspark-231c45677de0
  - MacOS: https://medium.com/@GalarnykMichael/install-spark-on-mac-pyspark-453f395f240b

#### 2. Get the data
  - Get the data from: https://drive.google.com/a/sjsu.edu/file/d/19RE5-Dv_q0yLH10eBEeNlic6vsqme_Uf/view?usp=sharing
  - Unzip 'data.zip' in root directory
  - Get stored graphs and utility matrix from: https://drive.google.com/open?id=1ffYzq1r0ObiVvNMFFswh4o1jSgH6Rcin
  - Unzip 'store.zip' in root directory
  - Create 'output' directory in root directory

#### 3. Run the code from the project root directory
  - For item based recommendation:<br/> 
    Usage:<br/> 
    `python item_recommender.py <group_id> <#_of_recommendations>`
    
  - For graph based recommendation:<br/> 
    Usage:<br/> 
    `python graph_recommender.py <group_id> <#_of_recommendations> [<executor_memory>] [<#_of_executors>]`
    
    Note: The sample.txt file contains sample group ids that can be used to test the above models.
