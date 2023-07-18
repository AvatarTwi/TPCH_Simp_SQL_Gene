from time import sleep

import numpy as np

if __name__ == '__main__':

    np.random.seed(1)
    for i in range(3):
        print(np.random.randint(0,10))
    np.random.seed(1)
    sleep(10)
    for i in range(3):
        print(np.random.randint(0,10))