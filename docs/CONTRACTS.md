# Supported types in ethereum abi

- uint
- int
- address
- bool
- string
- bytes

Abi should be in json. Example:

```json
{
  "inputs": [
    {
      "name": "state",
      "type": "uint256"
    },
    {
      "name": "author",
      "type": "address"
    }
  ],
  "name": "StateChange"
}
```

All the fields are mandatory. Other fields will be ignored.

# Supported conversions

## Eth -> Ton

| Ethereum type | Ton type   |
| ------------- | ---------- |
| Address       | Bytes      |
| Bytes         | Bytes      |
| Int           | Int        |
| Bool          | Bool       |
| String        | Bytes      |
| Array         | Array      |
| FixedBytes    | FixedBytes |
| FixedArray    | FixedArray |
| Tuple         | Tuple      |

## Ton -> Eth

| Ton type   | Ethereum type |
| ---------- | ------------- |
| FixedBytes | FixedBytes    |
| Bytes      | Bytes         |
| Uint       | Uint          |
| Int        | Int           |
| Bool       | Bool          |
| FixedArray | FixedArray    |
| Array      | Array         |
| Tuple      | Tuple         |
