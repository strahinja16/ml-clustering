from sklearn.cluster import KMeans
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib.pyplot import cm
from sklearn.decomposition import PCA
import numpy as np

# id, event_time,event_type,product_id,category_id,brand,price,user_id,user_session
PROCESSED_FILE_NAME = './processed_data1.csv'
N = 1000
CLUSTER_COUNT = 10
data = pd.read_csv(PROCESSED_FILE_NAME)
normalized_data = (data - data.mean())/data.std()
data = normalized_data
# data = data.drop(['day', 'weekday', 'hour'], axis=1)

X = data.values


km = KMeans(n_clusters=CLUSTER_COUNT, random_state=0)
y_km = km.fit_predict(X)
pca = PCA(random_state=1, n_components=3)
X = pca.fit_transform(X)
Xdf = pd.DataFrame(X)
Xdf['cluster'] = y_km


# # plot clusters
# color=cm.rainbow(np.linspace(0,1,CLUSTER_COUNT))
# for i, c in zip(range(0, CLUSTER_COUNT), color):
#     X_c = Xdf[Xdf['cluster'] == i]
#     plt.scatter(
#         X_c.values[:, 0], X_c.values[:, 1],
#         s=50, c=c,
#         marker='o', edgecolor='black',
#         label='cluster ' + str(i))    


# # plot the centroids
# centroid_pca = pca.transform(km.cluster_centers_)
# plt.scatter(
#     centroid_pca[:, 0], centroid_pca[:, 1],
#     s=250, marker='*',
#     c='red', edgecolor='black',
#     label='centroids'
# )
# plt.legend(scatterpoints=1)
# plt.grid()
# plt.show()


# X = data.values
# pca = PCA(random_state=1, n_components=3)
# X = pca.fit_transform(X)

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
color=cm.rainbow(np.linspace(0,1,CLUSTER_COUNT))

for i, c in zip(range(0, CLUSTER_COUNT), color):
    X_c = Xdf[Xdf['cluster'] == i].sample(n=N, random_state=0)
    xs = X_c.values[:, 0]
    ys = X_c.values[:, 1]
    zs = X_c.values[:, 2]
    ax.scatter(
        xs, ys, zs,
        s=50, c=c,
        marker='o', edgecolor='black',
        label='cluster ' + str(i))

centroid_pca = pca.transform(km.cluster_centers_)
ax.scatter(centroid_pca[:, 0], centroid_pca[:, 1],  centroid_pca[:, 2],
    s=250, marker='*',
    c='red', edgecolor='black',
    label='centroids')

plt.show()