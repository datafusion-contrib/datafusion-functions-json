# datafusion-functions-json

## Done

* [x] `json_contains(json: str, *keys: str | int) -> bool` - true if a JSON object has a specific key
* [x] `json_get(json: str, *keys: str | int) -> JsonUnion` - Get a value from a JSON object by its "path"
* [x] `json_get_str(json: str, *keys: str | int) -> str` - Get a string value from a JSON object by its "path"
* [x] `json_get_int(json: str, *keys: str | int) -> int` - Get an integer value from a JSON object by its "path"
* [x] `json_get_float(json: str, *keys: str | int) -> float` - Get a float value from a JSON object by its "path"
* [x] `json_get_bool(json: str, *keys: str | int) -> bool` - Get a boolean value from a JSON object by its "path"
* [x] `json_get_json(json: str, *keys: str | int) -> str` - Get any value from a JSON object by its "path", represented as a string
* [x] `json_length(json: str, *keys: str | int) -> int` - get the length of a JSON object or array

Cast expressions with `json_get` are rewritten to the appropriate method, e.g.

```sql
select * from foo where json_get(attributes, 'bar')::string='ham'
```
Will be rewritten to:
```sql
select * from foo where json_get_str(attributes, 'bar')='ham'
```

## TODO (maybe, if they're actually useful)

* [ ] `json_keys(json: str, *keys: str | int) -> list[str]` - get the keys of a JSON object
* [ ] `json_is_obj(json: str, *keys: str | int) -> bool` - true if the JSON is an object
* [ ] `json_is_array(json: str, *keys: str | int) -> bool` - true if the JSON is an array
* [ ] `json_valid(json: str) -> bool` - true if the JSON is valid
