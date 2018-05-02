
# RelaTree - Social Graph Analytics

# Import libraries 
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = (
	'--jars lib/graphframes-release-0-5-0-assembly-0.5.0-spark2.1.jar pyspark-shell')
import sys
sys.path.append('./lib')
import lib.graphframes as GF
import operator
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, size, lit, collect_list

def createContext(app_name="RelaTree", executor_memory="2g", no_of_executors="4"):
	''' 
	Function: Create spark context
	Parameters: app_name, executor_memory, no_of_executors
	Returns: Spark SQLContext
	'''
	sparkConf = (SparkConf().setMaster("local").setAppName(app_name).set("spark.executor.memory", executor_memory).set("spark.executor.instances", no_of_executors))
	sparkContext = SparkContext(conf=sparkConf)
	sql_context = SQLContext(sparkContext)
	return sql_context

def loadData(file_path,sql_context):
	'''
	Function: Create Spark dataframe from csv file
	Parameters: file_path,sql_context
	Returns: Pyspark Dataframe
	'''
	df = sql_context.read.format('com.databricks.spark.csv').options(header='true').load(file_path)
	return df

def getStats(df):
	'''
	Function: Provide stats for a Pyspark Dataframe
	Parameters: Pyspark Dataframe
	Returns: N/A
	'''
	print("\nRow count: %d\n\nColumn Count: %d\n\nColumn headers: %s\n\nSample Data:\n" %(df.count(),len(df.columns),df.columns))
	df.show(5)  

def createUserGraphVertices(df):
	'''
	Function: Create vertices dataframe
	Parameters: Pyspark Dataframe
	Returns: Pyspark dataframe depciting vertices
	'''
	print("Creating User Vertices DataFrame..")
	# Select user_Id [vertices] column from Spark dataframe
	df_users = df.select(['user_id'])
	df_users = df_users.selectExpr("user_id as id") 
	# Remove duplicate user_id entries and create vertices dataframe
	vertices = df_users.drop_duplicates()
	print("User Vertices DataFrame creation complete.")
	return vertices

def createUserGraphEdges(df):
	'''
	Function: Create edges dataframe
	Parameters: Pyspark Dataframe
	Returns: Pyspark dataframe depciting edges
	'''
	# Create the edges dataframe
	print("Creating User Edges DataFrame..")
	edges = df.select(col('user_id').alias('src'),col('group_id')).join(df.select(col('user_id').alias('dst'),col('group_id')), on=['group_id'], how='outer')
	# Remove duplicate entries
	edges = edges.drop_duplicates()
	print("User Edges DataFrame creation complete.")
	return edges

def createGraph(vertices, edges):
	'''
	Function: Create graph
	Parameters: Pyspark Dataframe - vertices, edges
	Returns: GraphFrame
	'''
	print("Creating graph..")
	# Generate the graph
	graph = GF.GraphFrame(vertices, edges)
	print("Graph creation complete.")
	return graph

def saveGraph(graph, name):
	'''
	Function: Save graph to file
	Parameters: GraphFrame
	Returns: N/A
	'''
	# Save the graph to a file
	print("Saving graph to file..")
	graph.vertices.write.parquet('store/'+name+'Vertices.parquet')
	graph.edges.write.parquet('store/'+name+'Edges.parquet')
	print("Graph has been saved successfully.")

def loadGraph(context, name):
	'''
	Function: Load graph from file
	Parameters: N/A
	Returns: GraphFrame
	'''
	# Load the graph from file
	print("\nLoading graph data..")
	vertices = context.read.parquet('store/'+name+'Vertices.parquet')
	edges = context.read.parquet('store/'+name+'Edges.parquet')
	print("\nGenerating graph..")
	graph = GF.GraphFrame(vertices, edges)
	print("\nGraph load complete.")
	return graph

def firstConnects(graph, vertex):
	'''
	Function: Obtain the first connects of a given vertex
	Parameters: GraphFrame, vertex label
	Returns: GraphFrame of connected vertices and their edges
	'''
	first_connect_motifs = graph.find("(v1)-[e]->(v2)").filter("v1.id == '"+vertex+"'")
	return first_connect_motifs.select("v2.id","e.group_id")

def cleanUp(df_list):
	'''
	Function: Delete dataframe to free memory
	Parameters: List of DataFrames
	Returns: N/A
	'''
	for df in df_list:
		del df
	print("\nDataFrame clean up complete.")

def getUsersOfAGroup(graph, group):
	'''
	Function: Get users connected by the given group
	Parameters: Graphframe, group_id
	Returns: Set of users
	'''
	print("\nGetting Users..")
	edges = graph.edges.filter("group_id = '"+group+"'").collect()
	users = set()
	for row in edges:
		users.add(row.src)
	return users

def addFirstConnects(graph, users):
	'''
	Function: Add first connected users to the given user set
	Parameters: Graphframe, users set
	Returns: Set of users
	'''
	print("\nAdding first connects..")
	newUsers = set()
	for user in users:
		v = firstConnects(graph, user)
		v = v.collect()
		for row in v:
			newUsers.add(row.id)
	users = users.union(newUsers)
	return users

def getGroupsAssociatedToUsers(graph, users):
	'''
	Function: Get all the groups associated with the given users
	Parameters: Graphframe, users set
	Returns: Dictionary of Groups with their counts frequency
	'''
	print("\nGetting groups..")
	groups = dict()
	for user in users:
		g = graph.edges.filter("src = '"+user+"'").collect()
		for row in g:
			key = row.group_id
			if key in groups:
				groups[key] += 1
			else:
				groups[key] = 1
	total = 0
	for key in groups.keys():
		total += groups[key]
	for key in groups.keys():
		groups[key] = groups[key] / total
	return groups

def getChannelsAssociatedToGroups(df, groups):
	'''
	Function: Get all the channels associated with the given groups
	Parameters: Pyspark Dataframe, groups dict
	Returns: Dictionary of Channels with their counts frequency weighted based on groups
	'''
	print("\nGetting channels..")
	channels = dict()
	for group in groups.keys():
		c = df.filter("group_id = '"+group+"'").collect()
		for row in c:
			key = row.channel_id
			if key in channels:
				channels[key] += groups[group]
			else:
				channels[key] = groups[group]
	total = 0
	for key in channels.keys():
		total += channels[key]
	for key in channels.keys():
		channels[key] = channels[key] / total
	return channels

def getChannelsForGroup(df, graph, group):
	'''
	Function: Get all the channels associated with the given group along with their respective importance factor
	Parameters: Pyspark Dataframe, Graphframe, group_id
	Returns: Dictionary of Channels with their counts frequency weighted based on groups
	'''
	users = getUsersOfAGroup(graph, group)
	users = addFirstConnects(graph, users)
	groups = getGroupsAssociatedToUsers(graph, users)
	channels = getChannelsAssociatedToGroups(df, groups)
	return channels

def createChannelGraphVetrices(df):
	'''
	Function: Create channel vertices dataframe
	Parameters: Pyspark Dataframe
	Returns: Pyspark dataframe depciting channel vertices
	'''
	print("Creating Channel Vertices DataFrame..")
	# Select channel_id [vertices] column from Spark dataframe
	df_channels = df.select(['channel_id'])
	df_channels = df_channels.selectExpr('channel_id as id') 
	# Remove duplicate user_id entries and create vertices dataframe
	vertices = df_channels.drop_duplicates()
	print("Channel Vertices DataFrame creation complete.")
	return vertices

def createChannelGraphEdges(df):
	'''
	Function: Create channel edges dataframe
	Parameters: Pyspark Dataframe
	Returns: Pyspark dataframe depciting channel edges
	'''
	# Create the edges dataframe
	print("Creating Channel Edges DataFrame..")
	channel_group = df.select(col('channel_id').alias('src'),col('group_id')).join(df.select(col('channel_id').alias('dst'),col('group_id')), on=['group_id'], how='outer')
	channel_group_filtered = channel_group.filter(channel_group.src != channel_group.dst)
	channel_group_count = channel_group_filtered.groupby(['src','dst']).count()
	channel_edges = channel_vertices.select(col('id').alias('src')).crossJoin(channel_vertices.select(col('id').alias('dst')))
	channel_edges = channel_edges.filter(channel_edges.src != channel_edges.dst)
	channel_edges_weighted = channel_edges.withColumn("init_weight", lit(1))
	channel_edge_weighted_joined = channel_edges_weighted.join(channel_group_count, on=['src','dst'], how='left_outer')
	channel_edge_weighted_joined = channel_edge_weighted_joined.na.fill(0)
	channel_edge_weighted_final = channel_edge_weighted_joined.withColumn('weight', channel_edge_weighted_joined['init_weight']+channel_edge_weighted_joined['count'])
	channel_edge_weighted_final = channel_edge_weighted_final.select('src','dst','weight')
	print("Channel Edges DataFrame creation complete.")
	return (channel_edge_weighted_final)

def graph_recommender(user_graph, channel_graph, df, group, k):
	'''
	Function: Get recommendations for the given group
	Parameters: User Graphframe, Channel Graphframe, Pyspark Dataframe, group_id, k (number of recommendations)
	Returns: List of tuples with channel_id and recommendation score
	'''
	print("\nFinding Recommendations..")
	channels = getChannelsForGroup(df, user_graph, group)
	recommendations = {}
	for channel in channels.keys():
		channel_motif = channel_graph.find("(v1)-[e]->(v2)").filter("v1.id == '"+channel+"'")
		direct_neighbors = channel_motif.select("v2.id","e.weight").collect()
		for row in direct_neighbors:
			if row.id not in channels:
				recommendations[row.id]=recommendations.get(row.id,0)+(row.weight*channels[channel])
	print("\nRecommendations found.")
	return (sorted(recommendations.items(), key=operator.itemgetter(1),reverse=True))[:k]

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print('Please specify the group_id you want recommendation for.')
	elif len(sys.argv) < 3:
		print('Please specify the number of recommendations you want.')
	else:
		try:
			k = int(sys.argv[2])
		except:
			print "Invalid number of recommendations."
		if len(sys.argv) > 3:
			try:
				# Create Spark context
				sql_context = createContext(app_name="RelaTree", executor_memory=sys.argv[3], no_of_executors=sys.argv[4])
			except:
				print "Invalid system arguments."
		else:
			sql_context = createContext(app_name="RelaTree")
		
		# Load group to member data into a Pyspark Dataframe
	#	 df_group_members = loadData('data/group_members.csv',sql_context)
		
		# Load group to channel data into a Pyspark Dataframe
		df_group_channels = loadData('data/group_channel.csv',sql_context)
		
		# Get stats on group members and group channels dataframes
	#	 getStats(df_group_members)
	#	 getStats(df_group_channels)
		
		# Create vertices dataframe
	#	 user_vertices = createUserGraphVertices(df_group_members)
	#	 channel_vertices = createChannelGraphVetrices(df_group_channels)

		# Create edges dataframe
	#	 user_edges = createEdges(df_group_members)
	#	 channel_edges = createChannelGraphEdges(df_group_channels)
		
		# Get stats on vertices and edges dataframes
	#	 getStats(user_vertices)
	#	 getStats(user_edges)
	#	 getStats(channel_vertices)
	#	 getStats(channel_edges)
		
		# Create Graph
	#	 duta_user_graph = createGraph(vertices,edges)
	#	 duta_channel_graph = createGraph(channel_vertices,channel_edges)
		
		# Clean up memory
	#	 cleanUp([user_vertices, user_edges, channel_vertices, channel_edges, df_group_members])
		
		# Save Graph to file 
	#	 saveGraph(duta_user_graph, 'userGraph)
	#	 saveGraph(duta_channel_graph, 'channelGraph')	
		
		# Load graph from file
		duta_user_graph = loadGraph(sql_context, 'userGraph')
		duta_channel_graph = loadGraph(sql_context, 'channelGraph')
		result = graph_recommender(duta_user_graph, duta_channel_graph, df_group_channels, sys.argv[1], k)
		if len(result) < 1:
			print('Invalid group_id.')
		else:
			# Saving Recommendations to File
			with open('output/graph_recommendations_for_'+str(sys.argv[1])+'.csv','w') as f:
			f.write('channel_id,score\n')
			for r in result:
				f.write(str(r[0])+','+str(r[1])+ '\n')
			print('Recommendations saved to file named - graph_recommendations_for_'+str(sys.argv[1])+'.csv in output folder.')

