# -*- coding: utf-8 -*-

import os
from sklearn.cluster import MiniBatchKMeans
import pandas as pd
from dask.distributed import Client , LocalCluster
from sklearn.metrics import silhouette_score
from scipy.spatial.distance import euclidean
import matplotlib.pyplot as plt


# Load data from X_file_path csv file and return transposed pandas dataframe
def load_X_data(X_file_path):
    
    try:
        # Load data from the csv file  including  the header into pandas dataframe
        df_X = pd.read_csv(X_file_path, sep=',' )  
        print ( "X  type ", type(df_X))
                
        X_transposed = df_X.transpose()       
        print ( "X_transposed sliced dataframe shape", X_transposed.shape)
        
        return X_transposed

    except Exception as e:
        print("Error occurred while loading descriptors CSV data:", e)
        
        
        
# Load data from y_file_path csv file and return transposed pandas dataframe      
def load_y_data(y_file_path):
    
    try:
       
        # Load data from the CSV file  including the header using pandas dataframe  
        df_Y = pd.read_csv(y_file_path, sep= ',' )
        print ( "y  shape ", df_Y.shape)
        
        # exclude the first column and first row
        df_Y = df_Y.iloc[:, 1:]      
        print ( "y sliced dataframe shape", df_Y.shape)
        print ( "y sliced dataframe", df_Y)
        
        Y_transposed = df_Y.transpose()
        print ( "Y_transposed sliced dataframe shape", Y_transposed.shape)
        
        return  Y_transposed

    except Exception as e:
        print("Error occurred while loading concentrations CSV data:", e)
        

# Combine X and Y datasets
def combine_datasets(X_file_path, y_file_path):
    
    X = load_X_data(X_file_path)
    Y = load_y_data(y_file_path)
    
    combined_data = pd.concat([X, Y], axis=0)
    print ( "combined_data shape", combined_data.shape)   
    
    return combined_data
        
        
# Determine the optimal number of clusters
def optimal_cluster_number(combined_data, min_clusters, max_clusters):
    
    best_silhouette_score  = -1
    best_num_clusters = min_clusters
    
    for n_clusters in range(min_clusters, max_clusters + 1):
        
        kmeans = MiniBatchKMeans(n_clusters= n_clusters, random_state=42)
        cluster_labels = kmeans.fit_predict(combined_data)
        
        # Compute silhouette score
        silhouette = silhouette_score(combined_data, cluster_labels)
        print ( f"silhouette {n_clusters} ", silhouette)
        
        if silhouette > best_silhouette_score:
            
            best_silhouette_score = silhouette
            print ( "best_silhouette_score ", best_silhouette_score)
            
            best_num_clusters = n_clusters
            print ( "best_num_clusters ", best_num_clusters)
            
    return best_num_clusters


        
# Perform MiniBatchKMeans clustering
def perform_clustering(combined_data, num_clusters):
    
    kmeans = MiniBatchKMeans(n_clusters=num_clusters, random_state=42)
    
    cluster_labels = kmeans.fit_predict(combined_data)
    # print ( "cluster_labels ", cluster_labels)
    
    silhouette = silhouette_score(combined_data, cluster_labels)
    print ( "silhouette score ", silhouette)
    
    return cluster_labels

                              
# Find the cluster containing Y
def find_y_cluster(cluster_labels):
    
    # Assuming Y is the last row
    y_cluster = cluster_labels[-1] 
    print ( "y_cluster ", y_cluster)
    
    return y_cluster 

                              

# Calculate distances between Y and data points in the same cluster
def calculate_distances(combined_data,  cluster_labels, cluster_id):
    
    cluster_indices = [i for i, label in enumerate(cluster_labels) if label == cluster_id]
    print ( "cluster_indices type ", type(cluster_indices))
    
    
    distances = []
    
    for idx in cluster_indices:
        
        distances.append((idx, euclidean(combined_data.iloc[idx].values, combined_data.iloc[-1].values.flatten()))) 
  
        
    sorted_distances = sorted(distances, key=lambda x: x[1])    
    print ( "sorted_distances ", sorted_distances)
    print ( "sorted_distances type ", type(sorted_distances))
    print ( "sorted_distances length", len(sorted_distances))
    
    
    return sorted_distances



# Select features closest to Y
def select_closest_features(X_file_path, distances, num_features):
     
    X = load_X_data(X_file_path)
    selected_features = []    
    
    for idx, _ in distances[:min(len(distances), num_features)]:
        
        if 0 <= idx < len(X):
        
            selected_features.append(X.iloc[idx])
            
        else:
            print(f"Index {idx} is out of bounds for DataFrame X")
        
    selected_features = pd.DataFrame(selected_features)
    # print ( "selected_features ", selected_features)
    print ( "selected_features type ", type(selected_features))
    print ( "selected_features shape ", selected_features.shape)
    
    selected_features = selected_features.transpose()
    print ( "transposed selected_features  shape ", selected_features.shape)
        
    return selected_features

                    


# Function gets the pandas dataframe  and write it to csv file and returns file path dictionaty    
def write_to_csv(X_selected, output_path):
    
       
    
    # Create a separate directory for the output file
    try:
        
      # Create the output directory if it doesn't exist                                                        
       os.makedirs(output_path, exist_ok = True)     
       file_name = 'miniBatchKMeans.csv'
       file_path = os.path.join(output_path, file_name)
                
       X_selected.to_csv(file_path, sep = ',', header =True, index = True ) 

       file_path_dict = {'miniBatchKMeans': file_path}
       print("CSV file written successfully.")
       print ("CSV file size is  " , os.path.getsize(file_path))
       print ("CSV file column number is  " , X_selected.shape[1])
       print ("file_path_dictionary is  " , file_path_dict)
       return file_path 
   
    except Exception as e:
       print("Error occurred while writing matrices to CSV:", e)
       

    
        
if __name__ == '__main__': 
                 
    
    # Create Lucalluster with specification for each dask worker to create dask scheduler     
    cluster = LocalCluster (n_workers= 4,  threads_per_worker= 128, memory_limit='500GB',  timeout= 3000)   
    
    #Create the Client using the cluster
    client = Client(cluster) 

       
    X_file_path = r'/features.csv' 
    y_file_path = r'/endpoint.csv'  
    output_patoutput' 
    
    # Determine the optimal number of clusters
    min_clusters = 2
    max_clusters = 10
    
    # num_clusters = 2
    
    combined_data = combine_datasets(X_file_path , y_file_path )
  
    num_clusters = optimal_cluster_number(combined_data, min_clusters, max_clusters)
    
    # Perform MiniBatchKMeans clustering
    cluster_labels = perform_clustering(combined_data, num_clusters)
    
    # Find the cluster containing Y
    y_cluster = find_y_cluster(cluster_labels)
    
    # Calculate distances between Y and data points in the same cluster
    distances = calculate_distances(combined_data,  cluster_labels, y_cluster)
    
    # Select features closest to Y
    num_closest_features = 10000

    closest_features = select_closest_features(X_file_path, distances, num_closest_features)
    
    path = write_to_csv(closest_features, output_path)    
    print ('path', path )    
 
    scheduler_address = client.scheduler.address
    print("Scheduler Address:", scheduler_address)
    print(cluster.workers)
    print(cluster.scheduler)
    print(cluster.dashboard_link)
    print(cluster.status)
   
    client.close()
    cluster.close()       
    

            
