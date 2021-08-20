import sys

f1 = sys.argv[1]
f2 = sys.argv[2]

with open(f1, "rb") as fp:
    data1 = fp.read()

with open(f2, "rb") as fp:
    data2 = fp.read()

for i, (a, b) in enumerate(zip(data1, data2), 1):
    if a == b:
        continue

    for bit in range(8):
        mask = (0x1) << bit
        am = (a & mask) >> bit
        bm = (b & mask) >> bit
        if am != bm:
            print(f"Mismatch: {am} != {bm} for bit {(i-1)*8 + bit}")
