KMEANSSPARK=/home/hadoop/CloudProgrammingTonellotto/kmeansSpark

# d=3 k = 7
python3 ${KMEANSSPARK}/kmeans.py 3 7 input_data_d\=3_n\=1000.txt spark_centroids_d\=3_n\=1000_k\=7.txt > "${KMEANSSPARK}/testlog/d=3_n=1000_k=7.log"
python3 ${KMEANSSPARK}/kmeans.py 3 7 input_data_d\=3_n\=10000.txt spark_centroids_d\=3_n\=10000_k\=7.txt > "${KMEANSSPARK}/testlog/d=3_n=10000_k=7.log"
python3 ${KMEANSSPARK}/kmeans.py 3 7 input_data_d\=3_n\=100000.txt spark_centroids_d\=3_n\=100000_k\=7.txt > "${KMEANSSPARK}/testlog/d=3_n=100000_k=7.log"

# d=3 k = 13
python3 ${KMEANSSPARK}/kmeans.py 3 13 input_data_d\=3_n\=1000.txt spark_centroids_d\=3_n\=1000_k\=13.txt > "${KMEANSSPARK}/testlog/d=3_n=1000_k=13.log"
python3 ${KMEANSSPARK}/kmeans.py 3 13 input_data_d\=3_n\=10000.txt spark_centroids_d\=3_n\=10000_k\=13.txt > "${KMEANSSPARK}/testlog/d=3_n=10000_k=13.log"
python3 ${KMEANSSPARK}/kmeans.py 3 13 input_data_d\=3_n\=100000.txt spark_centroids_d\=3_n\=100000_k\=13.txt > "${KMEANSSPARK}/testlog/d=3_n=100000_k=13.log"

# d = 7 k = 7
python3 ${KMEANSSPARK}/kmeans.py 7 7 input_data_d\=7_n\=1000.txt spark_centroids_d\=7_n\=1000_k\=7.txt > "${KMEANSSPARK}/testlog/d=7_n=1000_k=7.log"
python3 ${KMEANSSPARK}/kmeans.py 7 7 input_data_d\=7_n\=10000.txt spark_centroids_d\=7_n\=10000_k\=7.txt > "${KMEANSSPARK}/testlog/d=7_n=10000_k=7.log"
python3 ${KMEANSSPARK}/kmeans.py 7 7 input_data_d\=7_n\=100000.txt spark_centroids_d\=7_n\=100000_k\=7.txt > "${KMEANSSPARK}/testlog/d=7_n=100000_k=7.log"

# d = 7 k = 13
python3 ${KMEANSSPARK}/kmeans.py 7 13 input_data_d\=7_n\=1000.txt spark_centroids_d\=7_n\=1000_k\=13.txt > "${KMEANSSPARK}/testlog/d=7_n=1000_k=13.log"
python3 ${KMEANSSPARK}/kmeans.py 7 13 input_data_d\=7_n\=10000.txt spark_centroids_d\=7_n\=10000_k\=13.txt > "${KMEANSSPARK}/testlog/d=7_n=10000_k=13.log"
python3 ${KMEANSSPARK}/kmeans.py 7 13 input_data_d\=7_n\=100000.txt spark_centroids_d\=7_n\=100000_k\=13.txt > "${KMEANSSPARK}/testlog/d=7_n=100000_k=13.log"
