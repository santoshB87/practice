"""
Dutch National Flag Problem
Given an array of size N containing only 0s, 1s, and 2s; sort the array in ascending order.
Your Task:
You don't need to read input or print anything. Your task is to complete the function sort012() that takes an array arr and N as input parameters and sorts the array in-place.


Expected Time Complexity: O(N)
Expected Auxiliary Space: O(1)


Constraints:
1 <= N <= 10^6
0 <= A[i] <= 2
"""

def sort012(arr, n):
    i = j = 0
    e = n-1
    while j <= e:
        if arr[j] < 1:
            arr[i], arr[j] = arr[j], arr[i]
            i = i + 1
            j = j + 1
        elif arr[j] > 1:
            arr[j], arr[e] = arr[e], arr[j]
            e = e - 1
        else:
            j = j + 1


arr = [0, 1, 2, 2, 1, 0, 0, 2, 0, 1, 1, 0]
sort012(arr, 12)
print(arr)