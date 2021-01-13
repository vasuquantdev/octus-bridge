import requests

got = []
for i in range(0, 3):
    port = 10000
    adddr = "http://127.0.0.1:{}/status/relay".format(port + i)
    data = requests.get(adddr).json()
    new = {}
    for k, v in data.items():
        hashes = set()
        for obj in v:
            hashes.add(obj['tx_hash'])
        new[k] = hashes
    got.append(new)

for data in got:
    for other in got:
        if data != other:
            print("Diffs!")
