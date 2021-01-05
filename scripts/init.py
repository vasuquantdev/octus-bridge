import requests

for i in range(0, 13):
    port = 10000
    adddr = "http://127.0.0.1:{}/init".format(port + i)
    data = {
        "ton_seed": "regret neutral swing truth shoe bean tunnel stone inform say equip sand",
        "eth_seed": "regret neutral swing truth shoe bean tunnel stone inform say equip sand",
        "password": "12345678",
        "language": "en",
    }
    res = requests.post(adddr, json=data)
    print(adddr, res.text)
