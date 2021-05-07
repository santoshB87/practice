arr = [7,10,4,3,20,15]
k = 3
n=6
def kthSmallest(arr, k, n):
    # Minimum and maximum element from the array
    low = min(arr)
    high = max(arr)

    # Modified binary search
    while (low <= high):

        mid = low + (high - low) // 2

        # To store the count of elements from the array
        # which are less than mid and
        # the elements which are equal to mid
        countless = 0
        countequal = 0

        for i in range(n):

            if (arr[i] < mid):
                countless += 1

            elif (arr[i] == mid):
                countequal += 1

                # If mid is the kth smallest
        if (countless < k and (countless + countequal) >= k):
            return mid

            # If the required element is less than mid
        elif (countless >= k):
            high = mid - 1

            # If the required element is greater than mid
        elif (countless < k and countless + countequal < k):
            low = mid + 1

print(kthSmallest(arr, k, n))