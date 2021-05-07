def equilibriumPoint(A, N):
    total = sum(A)
    rightsum = 0
    result = []
    for i in reversed(range(N)):
        if rightsum == total - (rightsum + A[i]):
            result.append(i+1)
        else:
            rightsum = rightsum + A[i]
    print(result)

equilibriumPoint([1],1)