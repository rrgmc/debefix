# debefix - Database seeding and fixtures

## Sample input

The configuration can be in a single or multiple files, the file itself doesn't matter. The file names/directories are 
sorted alphabetically, so the order can be deterministic.

The same table can also be present in multiple files, given that the `config` section is equal (or only set in one of them).

```yaml
tags:
  config:
    table_name: "public.tag"
  rows:
    - tag_id: !dbfexpr "generated" # means that this will be generated, for example as a database autoincrement
      name: "Go"
      created_at: !!timestamp 2023-01-01T12:30:12Z
      updated_at: !!timestamp 2023-01-01T12:30:12Z
      _dbfconfig:
        id: "go" # refid to be targeted by '!dbfexpr "refid:tags:go:tag_id"'
    - tag_id: !dbfexpr "generated"
      name: "JavaScript"
      created_at: !!timestamp 2023-01-01T12:30:12Z
      updated_at: !!timestamp 2023-01-01T12:30:12Z
      _dbfconfig:
        id: "javascript"
    - tag_id: !dbfexpr "generated"
      name: "C++"
      created_at: !!timestamp 2023-01-01T12:30:12Z
      updated_at: !!timestamp 2023-01-01T12:30:12Z
      _dbfconfig:
        id: "cpp"
users:
  config:
    table_name: "public.user"
  rows:
    - user_id: 1
      name: "John Doe"
      email: "john@example.com"
      created_at: !!timestamp 2023-01-01T12:30:12Z
      updated_at: !!timestamp 2023-01-01T12:30:12Z
      _dbfconfig:
        id: "johndoe" # refid to be targeted by '!dbfexpr "refid:users:johndoe:user_id"'
    - user_id: 2
      name: "Jane Doe"
      email: "jane@example.com"
      created_at: !!timestamp 2023-01-04T12:30:12Z
      updated_at: !!timestamp 2023-01-04T12:30:12Z
      _dbfconfig:
        id: "janedoe"
posts:
  config:
    table_name: "public.post"
  rows:
    - post_id: 1
      title: "Post 1"
      text: "This is the text of the first post"
      user_id: !dbfexpr "refid:users:johndoe:user_id"
      created_at: !!timestamp 2023-01-01T12:30:12Z
      updated_at: !!timestamp 2023-01-01T12:30:12Z
      _dbfdeps:
        posts_tags: # declaring tables in _dbfdeps is exactly the same as declaring top-level, but allows using "parent" expression to get parent info
          rows:
            - post_id: !dbfexpr "parent:post_id"
              tag_id: !dbfexpr "refid:tags:go:tag_id"
      _dbfconfig:
        id: "post_1"
    - post_id: 2
      parent_post_id: !dbfexpr "refid:posts:post_1:post_id" # order matters, to self-referential fields must be added in order
      title: "Post 2"
      text: "This is the text of the seco d post"
      user_id: !dbfexpr "refid:users:johndoe:user_id"
      created_at: !!timestamp 2023-01-02T12:30:12Z
      updated_at: !!timestamp 2023-01-02T12:30:12Z
      _dbfdeps:
        posts_tags:
          rows:
            - post_id: !dbfexpr "parent:post_id"
              tag_id: !dbfexpr "refid:tags:javascript:tag_id" # tag_id is generated so the value will be resolved before being set here 
        comments:
          rows:
            - comment_id: 3
              post_id: !dbfexpr "parent:post_id"
              user_id: !dbfexpr "refid:users:janedoe:user_id"
              text: "I liked this post!"
posts_tags:
  config:
    table_name: "public.post_tag"
comments:
  config:
    depends:
      - posts # add a manual dependency if there is not refid linking these tables
  rows:
    - comment_id: 1
      post_id: 1
      user_id: !dbfexpr "refid:users:janedoe:user_id"
      text: "Good post!"
      created_at: !!timestamp 2023-01-01T12:31:12Z
      updated_at: !!timestamp 2023-01-01T12:31:12Z
    - comment_id: 2
      post_id: 1
      user_id: !dbfexpr "refid:users:johndoe:user_id"
      text: "Thanks!"
      created_at: !!timestamp 2023-01-01T12:35:12Z
      updated_at: !!timestamp 2023-01-01T12:35:12Z
```

### Author

Rangel Reale (rangelreale@gmail.com)
