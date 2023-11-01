import numpy as np

def non_vec(array=[1]):
    return [x * x for x in array]
print(non_vec([1,2,3,4,5])) #[1, 4, 9, 16, 25]

def vec(array=[1]):
    np_array = np.array(array)
    return (np_array * np_array)
print(vec([1,2,3,4,5]))
