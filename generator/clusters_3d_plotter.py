from sklearn.datasets import make_blobs
from mpl_toolkits.mplot3d import Axes3D
from pprint import pprint

import matplotlib.pyplot as plt
import numpy as np
import argparse

import clusters_generator as gen
import points_io as io

    
def show_3d_plot(points, labels, k, centers=None):

    fig = plt.figure()
    ax = Axes3D(fig)

    for label in range(0, k):
        xs = points[labels == label, 0]
        ys = points[labels == label, 1]
        zs = points[labels == label, 2]

        ax.scatter(xs, ys, zs)

    if centers is not None:
        c_xs = centers[:,0]
        c_ys = centers[:,1]
        c_zs = centers[:,2]

        ax.scatter(c_xs, c_ys, c_zs, color="black")

    plt.show()


if __name__ == '__main__':

    np.random.seed(0)

    parser = argparse.ArgumentParser("clusters_generator",
            description="Plot n points k clusters in 3D.")
    parser.add_argument("n", type=int, help="number of points")
    parser.add_argument("d", type=int, help="dimension of points [3]")
    parser.add_argument("k", type=int, help="number of centers")
    args = parser.parse_args()

    n = args.n
    d = args.d = 3
    k = args.k
    
    points, labels, centers = gen.generate_clusters(n, d, k)
    centroids = None

    show_3d_plot(points, labels, k, centroids)