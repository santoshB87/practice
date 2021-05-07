import math
N = 8
K = 5
arr = [1,2,3,4,5,6,8,9]
i = 0
i=0
while i < N:
    start = i
    end =  min(i + K - 1, N - 1)
    if end >= N:
        end = N -1
    while end > start:
        arr[start], arr[end] = arr[end], arr[start]
        start = start + 1
        end = end - 1
    i = i + K
print(arr)

