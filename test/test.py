

import os
import matplotlib.pyplot as plt
import numpy as np


n = 4
X = np.arange(n)
Y = (1 - X /float(n)) * np.random.uniform(0.5, 1.0, n)

plt.bar(X, Y)

print(Y)

for x,y in zip(X, Y):
    plt.text(x, y, '%.2f'%y,ha='center', va='bottom')


plt.savefig('./test.png')

#plt.show()

#os.system('./test1 > test.log')