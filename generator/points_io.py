import numpy as np


def save_to_file(points, n, d, k):

    output_file_name = f"data_n={n}_d={d}_k={k}.txt"

    with open(output_file_name, "w") as output_file:
        for point in points:
            output_file.write(" ".join(str(coordinate) for coordinate in point) + "\n")

    print(f"Points saved to '{output_file_name}'")


def read_points_from_file(input_file_name):

    points = []

    with open(input_file_name, "r") as input_file:
        for line in input_file:
            point = [float(coordinate) for coordinate in line.rstrip().split(" ")]
            points.append(point)

    return np.asarray(points, dtype=np.float64)


def read_ids_and_points_from_file(input_file_name):

    points = []

    with open(input_file_name, "r") as input_file:
        for line in input_file:
            cluster_id, point_str = line.rstrip().split("\t")
            point = [float(coordinate) for coordinate in point_str.split(" ")]
            points.append(point)

    return cluster_id, np.asarray(points, dtype=np.float64)
