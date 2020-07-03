from sklearn.datasets import make_blobs
from mpl_toolkits.mplot3d import Axes3D
from pprint import pprint

import matplotlib.pyplot as plt
import argparse

# doc: https://scikit-learn.org/stable/modules/generated/sklearn.datasets.make_blobs.html#sklearn.datasets.make_blobs

def generate_clusters(n, d, k, show=True):

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
    
    if show:
        if d == 2:
            show_2d_plot(points, labels, k)

        if d == 3:
            show_3d_plot(points, labels, k)

    return points, labels, centers
    

def show_2d_plot(points, labels, k):

    for label in range (0, k):
        plt.scatter(points[labels == label, 0], points[labels == label, 1], s=10)
        plt.scatter(points[labels == label, 0], points[labels == label, 1], s=10)

    plt.show()
    
    
def show_3d_plot(points, labels, k):
    fig = plt.figure()
    ax = Axes3D(fig)

    for label in range (0, k):
        xs = points[labels == label, 0]
        ys = points[labels == label, 1]
        zs = points[labels == label, 2]

        ax.scatter(xs, ys, zs)

    plt.show()


def save_to_file(points, n, d, k):
    output_file_name = f"data_n={n}_d={d}_k={k}.txt"

    with open(output_file_name, "w") as output_file:
        for point in points:
            output_file.write(" ".join(str(coordinate) for coordinate in point) + "\n")

    print(f"Points saved to '{output_file_name}'")


if __name__ == '__main__':
    parser = argparse.ArgumentParser("clusters_generator",
            description="Generate n points of dimension d belonging to k clusters.")
    parser.add_argument("n", type=int, help="number of points")
    parser.add_argument("d", type=int, help="dimension of points")
    parser.add_argument("k", type=int, help="number of centers")
    args = parser.parse_args()

    n = args.n
    d = args.d
    k = args.k
    points, labels, centers = generate_clusters(n, d, k, show=False)

    save = True;
    if save:
        save_to_file(points, n, d, k)