# debefix - Database seeding and fixtures
[![GoDoc](https://godoc.org/github.com/rrgmc/debefix?status.png)](https://godoc.org/github.com/rrgmc/debefix)

### WARNNG: v1 is deprecated, use v2 instead.

debefix is a Go library to seed database data and/or create fixtures for DB tests.

Tables can reference each other using string ids (called `refid`), and generated fields (like database auto increment or
generated UUID) are supported and can be resolved and used by other table's references.

Dependencies between tables can be detected automatically by reference ids, or manually. This is used to generate a
dependency graph and output the insert statements in the correct order.

Using the YAML tag `!expr` it is possible to define expressions on field values.

Tables with rows can be declared at the top-level on inside a parent row using a special `!deps` tag. In this case,
values from the parent row can be used, using the `parent:<fieldname>` expression.

## Install

```shell
go get github.com/rrgmc/debefix
```

## Goals

- For developer seeding or test fixtures, not for inserting a huge amount of records.
- YAML files are to be manually edited, so they must be easy to read and write.
- Input can be files or memory, to allow creating simple tests.  

## Sample input

The configuration can be in a single or multiple files, the file itself doesn't matter. The file names/directories are
sorted alphabetically, so the order can be deterministic.

The same table can also be present in multiple files, given that the `config` section is equal (or only set in one of them).

Only files that have the extension `.dbf.yaml` are loaded by the directory loader.

```yaml
# all_data.dbf.yaml
tables:
  tags:
    config:
      table_name: "public.tag" # database table name. If not set, will use the table id (tags) as the table name.
    rows:
      - tag_id: !expr "generated:int" # means that this will be generated, for example as a database autoincrement
        _refid: !refid "go" # refid to be targeted by '!expr "refid:tags:go:tag_id"'. Field name is ignored.
        name: "Go"
        created_at: !!timestamp 2023-01-01T12:30:12Z
        updated_at: !!timestamp 2023-01-01T12:30:12Z
      - tag_id: !expr "generated:int"
        _refid: !refid "javascript"
        name: "JavaScript"
        created_at: !!timestamp 2023-01-01T12:30:12Z
        updated_at: !!timestamp 2023-01-01T12:30:12Z
      - tag_id: !expr "generated:int"
        _refid: !refid "cpp"
        name: "C++"
        created_at: !!timestamp 2023-01-01T12:30:12Z
        updated_at: !!timestamp 2023-01-01T12:30:12Z
  users:
    config:
      table_name: "public.user"
    rows:
      - user_id: 1
        _refid: !refid "johndoe" # refid to be targeted by '!expr "refid:users:johndoe:user_id"'. Field name is ignored.
        name: "John Doe"
        email: "john@example.com"
        created_at: !!timestamp 2023-01-01T12:30:12Z
        updated_at: !!timestamp 2023-01-01T12:30:12Z
      - user_id: 2
        _refid: !refid "janedoe"
        name: "Jane Doe"
        email: "jane@example.com"
        created_at: !!timestamp 2023-01-04T12:30:12Z
        updated_at: !!timestamp 2023-01-04T12:30:12Z
  posts:
    config:
      table_name: "public.post"
    rows:
      - post_id: 1
        _refid: !refid "post_1"
        title: "Post 1"
        text: "This is the text of the first post"
        user_id: !expr "refid:users:johndoe:user_id"
        created_at: !!timestamp 2023-01-01T12:30:12Z
        updated_at: !!timestamp 2023-01-01T12:30:12Z
        deps:
          !deps
          posts_tags: # declaring tables in !deps is exactly the same as declaring top-level, but allows using "parent" expression to get parent info
            rows:
              - post_id: !expr "parent:post_id"
                tag_id: !expr "refid:tags:go:tag_id"
      - post_id: 2
        parent_post_id: !expr "refid:posts:post_1:post_id" # order matters, so self-referential fields must be set in order
        title: "Post 2"
        text: "This is the text of the seco d post"
        user_id: !expr "refid:users:johndoe:user_id"
        created_at: !!timestamp 2023-01-02T12:30:12Z
        updated_at: !!timestamp 2023-01-02T12:30:12Z
        deps:
          !deps
          posts_tags:
            rows:
              - post_id: !expr "parent:post_id"
                tag_id: !expr "refid:tags:javascript:tag_id" # tag_id is generated so the value will be resolved before being set here 
          comments:
            rows:
              - comment_id: 3
                post_id: !expr "parent:post_id"
                user_id: !expr "refid:users:janedoe:user_id"
                text: "I liked this post!"
  posts_tags:
    config:
      table_name: "public.post_tag"
  comments:
    config:
      depends:
        - posts # add a manual dependency if there is no refid linking the tables
    rows:
      - comment_id: 1
        post_id: 1
        user_id: !expr "refid:users:janedoe:user_id"
        text: "Good post!"
        created_at: !!timestamp 2023-01-01T12:31:12Z
        updated_at: !!timestamp 2023-01-01T12:31:12Z
      - comment_id: 2
        post_id: 1
        user_id: !expr "refid:users:johndoe:user_id"
        text: "Thanks!"
        created_at: !!timestamp 2023-01-01T12:35:12Z
        updated_at: !!timestamp 2023-01-01T12:35:12Z
```

## Field value expressions

- `!expr "refid:<table>:<refid>:<fieldname>"`: reference a **refid** field value in a table. This id is 
  declared using a `_refid: !refid "<refid>"` special tagged field in the row.
- `!expr "parent<:level>:<fieldname>"`: reference a field in the parent table. This can only be used inside a `!deps` 
  block. Level is the number of parent levels, if not specified the default value is 1.
- `!expr "calculated:type<:parameter>"`: calculate (generate) a field value from a callback.
- `!expr "generated<:type>"`: indicates that this is a generated field that must be supplied at resolve time, and can later
  be used by other references once resolved. If type is specified, the value is parsed/cast to this type after db retrieval.
  The default types are 'int', 'float', 'str' and 'timestamp', using the YAML formats.
- `!expr resolve:name`: calls a callback set with `WithNamedResolveCallback` at resolve time to resolve field value.

## Special fields

Some field tags are handled in a special way. **The name of the field is ignored**.

- `_refid: !refid "<refID>"`: sets the refID of a table row
- `_tags: !tags ["tag1", "tag2"]`: add tags to the table row
- `_deps: !deps {<tableID>: {...table config...}}`: add dependencies to the table row

## Generating SQL

SQL can be generated using `github.com/rrgmc/debefix/db/sql/<dbtype>`.

```go
import (
    "sql"

    dbsql "github.com/rrgmc/debefix/db/sql"
    "github.com/rrgmc/debefix/db/sql/postgres"
)

func main() {
    db, err := sql.Open("postgres", "dsn://postgres")
    if err != nil {
        panic(err)
    }

    // will send an INSERT SQL for each row to the db, taking table dependency in account for the correct order. 
    resolvedValues, err := postgres.GenerateDirectory(context.Background(), "/x/y", dbsql.NewSQLQueryInterface(db))
    if err != nil {
        panic(err)
    }
    
    // resolvedValues will contain all data that was inserted, including any generated fields like autoincrement.
}
```

## Generating Non-SQL

The import `github.com/rrgmc/debefix/db` contains a `ResolverFunc` that is not directly tied to SQL, it can be
used to insert data in any database that has the concepts of "tables" with a list of field/values.

As inner maps/arrays are supported by YAML, data with more complex structure should work without any problems.

# Samples

- [debefix-sample-app](https://github.com/rrgmc/debefix-sample-app): real-world blog microservice using debefix for
  seeding, test fixtures, and integration tests.
- [samples simple](https://github.com/rrgmc/debefix-samples/tree/master/simple): simple blog sample.
- [samples sakila](https://github.com/rrgmc/debefix-samples/tree/master/sakila): fixture sample using the "sakila" sample database.
- [samples mongodb](https://github.com/rrgmc/debefix-samples/tree/master/mongodb): MongoDB fixture sample.

## Extra

### Sub-packages

- [filter](https://pkg.go.dev/github.com/rrgmc/debefix/filter): simple methods to find and extract data from parsed or 
  resolved data, and doing transformations to objects, like entities. Can be used to get test data from fixtures.
- [value](https://pkg.go.dev/github.com/rrgmc/debefix/value): value parsers both for "Load" and "Resolve", like UUID.

### External

- [debefix-mongodb](https://github.com/rrgmc/debefix-mongodb): MongoDB fixture resolver.

# License

MIT

### Author

Rangel Reale (rangelreale@gmail.com)
