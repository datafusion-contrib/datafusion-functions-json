# datafusion-functions-json

[![CI](https://github.com/datafusion-contrib/datafusion-functions-json/actions/workflows/ci.yml/badge.svg?event=push)](https://github.com/datafusion-contrib/datafusion-functions-json/actions/workflows/ci.yml?query=branch%3Amain)
[![Crates.io](https://img.shields.io/crates/v/datafusion-functions-json?color=green)](https://crates.io/crates/datafusion-functions-json)

**Note:** This is not an official Apache Software Foundation release, see [datafusion-contrib/datafusion-functions-json#5](https://github.com/datafusion-contrib/datafusion-functions-json/issues/5).

This crate provides a set of functions for querying JSON strings in DataFusion. The functions are implemented as scalar functions that can be used in SQL queries.

To use these functions, you'll just need to call:

```rust
datafusion_functions_json::register_all(&mut ctx)?;
```
To register the below JSON functions in your `SessionContext`.

# Examples

```sql
-- Create a table with a JSON column stored as a string
CREATE TABLE test_table (id INT, json_col VARCHAR) AS VALUES
(1, '{}'),
(2, '{ "a": 1 }'),
(3, '{ "a": 2 }'),
(4, '{ "a": 1, "b": 2 }'),
(5, '{ "a": 1, "b": 2, "c": 3 }');

-- Check if each document contains the key 'b'
SELECT id, json_contains(json_col, 'b') as json_contains FROM test_table;
-- Results in
-- +----+---------------+
-- | id | json_contains |
-- +----+---------------+
-- | 1  | false         |
-- | 2  | false         |
-- | 3  | false         |
-- | 4  | true          |
-- | 5  | true          |
-- +----+---------------+

-- Get the value of the key 'a' from each document
SELECT id, json_col->'a' as json_col_a FROM test_table

-- +----+------------+
-- | id | json_col_a |
-- +----+------------+
-- | 1  | {null=}    |
-- | 2  | {int=1}    |
-- | 3  | {int=2}    |
-- | 4  | {int=1}    |
-- | 5  | {int=1}    |
-- +----+------------+
```


## Done

* [x] `json_contains(json: str, *keys: str | int) -> bool` - true if a JSON string has a specific key (used for the `?` operator)
* [x] `json_get(json: str, *keys: str | int) -> JsonUnion` - Get a value from a JSON string by its "path"
* [x] `json_get_str(json: str, *keys: str | int) -> str` - Get a string value from a JSON string by its "path"
* [x] `json_get_int(json: str, *keys: str | int) -> int` - Get an integer value from a JSON string by its "path"
* [x] `json_get_float(json: str, *keys: str | int) -> float` - Get a float value from a JSON string by its "path"
* [x] `json_get_bool(json: str, *keys: str | int) -> bool` - Get a boolean value from a JSON string by its "path"
* [x] `json_get_json(json: str, *keys: str | int) -> str` - Get a nested raw JSON string from a JSON string by its "path"
* [x] `json_get_array(json: str, *keys: str | int) -> array` - Get an arrow array from a JSON string by its "path"
* [x] `json_as_text(json: str, *keys: str | int) -> str` - Get any value from a JSON string by its "path", represented as a string (used for the `->>` operator)
* [x] `json_length(json: str, *keys: str | int) -> int` - get the length of a JSON string or array

- [x] `->` operator - alias for `json_get`
- [x] `->>` operator - alias for `json_as_text`
- [x] `?` operator - alias for `json_contains`
- [x] `#>` operator - `json_get` over a path, so `a #> array['x', 'y']` is `a -> 'x' -> 'y'`
- [x] `#>>` operator - `json_as_text` over a path, so `a #>> array['x', 'y']` is `a -> 'x' ->> 'y'`

The right-hand side of a path operator is a `text[]` list expression:

```sql
select json_col #>> array['a', 'b'] from test_table;
select json_col #>> array['a', 'b']::text[] from test_table;
select json_col #>> $1::text[] from test_table;
```

Postgres's array literal syntax, `'{a,b}'`, is not supported. As in postgres, each element is
a key when it reaches an object and an index when it reaches an array, so
`array['items', '0', 'name']` walks to `items`, then element zero, then `name`. A list of
integers is a path of indices, and a negative index counts from the end of the array. The
`json_get*` and `json_as_text` functions accept the same list as a path, e.g.
`json_as_text(json_col, array['a', 'b'])`.

### Notes
Cast expressions with `json_get` are rewritten to the appropriate method, e.g.

```sql
select * from foo where json_get(attributes, 'bar')::string='ham'
```
Will be rewritten to:
```sql
select * from foo where json_get_str(attributes, 'bar')='ham'
```

## TODO (maybe, if they're actually useful)

* [ ] `json_keys(json: str, *keys: str | int) -> list[str]` - get the keys of a JSON string
* [ ] `json_is_obj(json: str, *keys: str | int) -> bool` - true if the JSON is an object
* [ ] `json_is_array(json: str, *keys: str | int) -> bool` - true if the JSON is an array
* [ ] `json_valid(json: str) -> bool` - true if the JSON is valid
