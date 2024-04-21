# datafusion-functions-json

methods to implement:

* [x] `json_obj_contains(json: str, key: str) -> bool` - true if a JSON object has a specific key
* [ ] `json_obj_contains_all(json: str, keys: list[str]) -> bool` - true if a JSON object has all of a list of keys
* [ ] `json_obj_contains_any(json: str, keys: list[str]) -> bool` - true if a JSON object has all of a list of keys
* [ ] `json_obj_keys(json: str) -> list[str]` - get the keys of a JSON object
* [ ] `json_obj_values(json: str) -> list[Any]` - get the values of a JSON object
* [ ] `json_is_obj(json: str) -> bool` - true if the JSON is an object
* [ ] `json_array_contains(json: str, key: Any) -> bool` - true if a JSON array has a specific value
* [ ] `json_array_items(json: str) -> list[Any]` - get the items of a JSON array
* [ ] `json_is_array(json: str) -> bool` - true if the JSON is an array
* [ ] `json_get(json: str, key: str | int) -> Any` - get the value of a key in a JSON object or array
* [ ] `json_get_path(json: str, key: list[str | int]) -> Any` - is this possible?
* [ ] `json_length(json: str) -> int` - get the length of a JSON object or array
* [ ] `json_valid(json: str) -> bool` - true if the JSON is valid
* [ ] `json_cast(json: str) -> Any` - cast the JSON to a native type???
