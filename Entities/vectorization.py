import numpy as np

def vec(array=None):
    np_array = np.array(array)
    return np_array * np_array

print(vec([1, 2, 3, 4, 5]))  # [1, 4, 9, 16, 25]


def non_vec(array=None):
    return [x * x for x in array]

print(non_vec([1, 2, 3, 4, 5]))  # [1, 4, 9, 16, 25]
