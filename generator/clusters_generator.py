from sklearn.datasets import make_blobs
from mpl_toolkits.mplot3d import Axes3D
from pprint import pprint

import matplotlib.pyplot as plt
import numpy as np
import argparse

import points_io as io
import clusters_2d_plotter as plt2d
import clusters_3d_plotter as plt3d


# doc: https://scikit-learn.org/stable/modules/generated/sklearn.datasets.make_blobs.html#sklearn.datasets.make_blobs


def between(minv, val, maxv):
    return min(maxv, max(minv, val))


def generate_clusters(n, d, k):

    dirichlet = np.random.dirichlet(np.ones(k), size=(1,)).reshape(k)
    cluster_samples = [int(round(s)) for s in dirichlet * n]

    min_stdev = 0.1
    expected_stdev = 0.2
    max_stdev = 0.25
    cluster_stdevs = [(expected_stdev * s) / (n / k) for s in cluster_samples]
    cluster_stdevs = [between(min_stdev, d, max_stdev) for d in cluster_stdevs]

    points, labels, centers = make_blobs(
            n_samples=cluster_samples,
            n_features=d,
            centers=None,
            cluster_std=cluster_stdevs,
            center_box=(-1.0, 1.0),
            random_state=42,
            return_centers=True)

    return points, labels, centers


def generate_uniform_clusters(n, d, k):

    points, labels, centers = make_blobs(
            n_samples=n,
            n_features=d,
            centers=k,
            cluster_std=0.2,
            center_box=(-1.0, 1.0),
            random_state=42,
            return_centers=True)

    # points:       array of n points
    # points[i]:    array of d coordinates of the i-th point
    # labels:       array of n labels (to which i-th point belongs)
    # centers:      array of k centers
    # centers[i]:   array of d coordinates of the i-th center

    return points, labels, centers


if __name__ == '__main__':

    np.random.seed(0)

    parser = argparse.ArgumentParser("clusters_generator",
            description="Generate n points of dimension d belonging to k clusters.")
    parser.add_argument("n", type=int, help="number of points")
    parser.add_argument("d", type=int, help="dimension of points")
    parser.add_argument("k", type=int, help="number of centers")
    parser.add_argument("--show", 
                    dest="show",
                    default=False,
                    action="store_true",
                    help="show a plot of the generated data")

    args = parser.parse_args()

    n = args.n
    d = args.d
    k = args.k
    points, labels, centers = generate_clusters(n, d, k)

    save = True;
    if save:
        io.save_to_file(points, n, d, k)

    if args.show:

        if d == 2:
            plt2d.show_2d_clustered_plot(points, labels, k)

        if (d == 3):
            plt3d.show_3d_plot(points, labels, k)

