l = [1, 2, 0, 0, 3, 0, 6]
ln = len(l)
i=0
j=0
while i < ln:
    if l[i] == 0:
        if j == 0:
            j = i
        else:
            j = j+1
        while i < ln:
            if l[i] != 0:
                l[j] = l[i]
                l[i] = 0
                break
            i = i+1
    i = i+1
print (l)

