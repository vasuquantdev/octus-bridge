import requests

for i in range(0, 13):
    port = 10000
    adddr = "http://127.0.0.1:{}/unlock".format(port + i)
    data = {
        "password": "12345678"
    }
    res = requests.post(adddr, json=data)
    print(adddr, res.text)
