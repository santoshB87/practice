"""Given two arrays X and Y of positive integers, find the number of pairs such that xy > yx (raised to power of) where
x is an element from X and y is an element from Y."""


class Solution:

    # Function to count number of pairs such that x^y is greater than y^x.
    def countPairs(self, a, b, M, N):
        NoOfY = [0] * 5
        for i in range(N):
            if b[i] < 5:
                NoOfY[b[i]] += 1
        b.sort()
        total_pairs = 0
        for x in a:
            if x == 0:
                total_pairs += 0
                continue
            if x == 1:
                total_pairs += NoOfY[0]
                continue
            idx = bisect.bisect_right(b, x)
            ans = N - idx
            ans += NoOfY[0] + NoOfY[1]
            if x == 2:
                ans -= NoOfY[3] + NoOfY[4]
            if x == 3:
                ans += NoOfY[2]

            total_pairs += ans
            # print(total_pairs)
        return total_pairs


# {
#  Driver Code Starts
# Initial Template for Python 3

# Initial Template for Python 3

import atexit
import io
import sys
import bisect

_INPUT_LINES = sys.stdin.read().splitlines()
input = iter(_INPUT_LINES).__next__
_OUTPUT_BUFFER = io.StringIO()
sys.stdout = _OUTPUT_BUFFER


@atexit.register
def write():
    sys.__stdout__.write(_OUTPUT_BUFFER.getvalue())


if __name__ == '__main__':
    t = int(input())
    for i in range(t):
        M, N = map(int, input().strip().split())
        a = list(map(int, input().strip().split()))
        b = list(map(int, input().strip().split()))
        ob = Solution();
        print(ob.countPairs(a, b, M, N))
        # code here
# } Driver Code Ends