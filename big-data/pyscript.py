
##import the pretrained model
from pyspark.mllib.recommendation import MatrixFactorizationModel
loaded_als_model = MatrixFactorizationModel.load(sc, "./big-data")
##make recommendations by movie id
loaded_als_model.recommendProducts(15, 5)
##map movie titles to their ids to make recommendations by movie title
from pyspark.rdd import RDD
hdfs_file_path1 = "hdfs://localhost:9000/user/root/input/u.item"
movieTitle = itemRDD.map(lambda line: line.split("|")).map(lambda a:(float(a[0]),a[1])).collectAsMap()
# now start recommending films with their actual names
for p in loaded_als_model.recommendProducts(100, 5):
 print("For user: {}, we recommend: {}, rating: {}".format(str(p[0]), movieTitle[p[1]], str(p[2])))




