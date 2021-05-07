"""Given an array A of positive integers. Your task is to find the leaders in the array. An element of array is leader
if it is greater than or equal to all the elements to its right side. The rightmost element is always a leader. """


def leaders(A, N):
    leader = A[-1]
    result = []
    for i in reversed(range(N)):
        if leader <= A[i]:
            leader = A[i]
            result.append(leader)

    return reversed(result)


n = 6
A = [16,17,4,3,5,2]
leaders(A, n)
# Output: 17 5 2

