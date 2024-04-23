# datafusion-functions-json

## Done

* [x] `json_get(json: str, *keys: str | int) -> JsonUnion` - Get a value from a JSON object by it's "path"
* [x] `json_get_str(json: str, *keys: str | int) -> str` - Get a string value from a JSON object by it's "path"
* [x] `json_get_int(json: str, *keys: str | int) -> int` - Get an integer value from a JSON object by it's "path"
* [x] `json_get_float(json: str, *keys: str | int) -> float` - Get a float value from a JSON object by it's "path"
* [x] `json_get_bool(json: str, *keys: str | int) -> bool` - Get a boolean value from a JSON object by it's "path"
* [x] `json_get_json(json: str, *keys: str | int) -> str` - Get any value from a JSON object by it's "path", represented as a string
* [x] `json_obj_contains(json: str, key: str) -> bool` - true if a JSON object has a specific key

## TODO

* [ ] `json_obj_keys(json: str) -> list[str]` - get the keys of a JSON object
* [ ] `json_length(json: str) -> int` - get the length of a JSON object or array
* [ ] `json_obj_values(json: str) -> list[Any]` - get the values of a JSON object
* [ ] `json_obj_contains_all(json: str, keys: list[str]) -> bool` - true if a JSON object has all of the keys
* [ ] `json_obj_contains_any(json: str, keys: list[str]) -> bool` - true if a JSON object has any of the keys
* [ ] `json_is_obj(json: str) -> bool` - true if the JSON is an object
* [ ] `json_array_contains(json: str, key: Any) -> bool` - true if a JSON array has a specific value
* [ ] `json_array_items_str(json: str) -> list[Any]` - get the items of a JSON array
* [ ] `json_is_array(json: str) -> bool` - true if the JSON is an array
* [ ] `json_valid(json: str) -> bool` - true if the JSON is valid
* [ ] `json_cast(json: str) -> Any` - cast the JSON to a native type???
