package debefix

import (
	"testing/fstest"
	"time"
)

var testFS = fstest.MapFS{
	"base/users.dbf.yaml": &fstest.MapFile{
		Data: []byte(`users:
  config:
    table_name: "public.user"
  rows:
    - user_id: 1
      name: "John Doe"
      email: "john@example.com"
      created_at: !!timestamp 2023-01-01T12:30:12Z
      updated_at: !!timestamp 2023-01-01T12:30:12Z
      _dbfconfig:
        refid: "johndoe"
    - user_id: 2
      name: "Jane Doe"
      email: "jane@example.com"
      created_at: !!timestamp 2023-01-04T12:30:12Z
      updated_at: !!timestamp 2023-01-04T12:30:12Z
      _dbfconfig:
        refid: "janedoe"
        tags:
          - onlyone
`),
		ModTime: time.Now(),
	},
	"base/tags.dbf.yaml": &fstest.MapFile{
		Data: []byte(`tags:
  config:
    table_name: "tag"
  rows:
    - tag_id: !dbfexpr "generated"
      name: "Go"
      created_at: !!timestamp 2023-01-01T12:30:12Z
      updated_at: !!timestamp 2023-01-01T12:30:12Z
      _dbfconfig:
        refid: "go"
    - tag_id: !dbfexpr "generated"
      name: "JavaScript"
      created_at: !!timestamp 2023-01-01T12:30:12Z
      updated_at: !!timestamp 2023-01-01T12:30:12Z
      _dbfconfig:
        refid: "javascript"
    - tag_id: !dbfexpr "generated"
      name: "C++"
      created_at: !!timestamp 2023-01-01T12:30:12Z
      updated_at: !!timestamp 2023-01-01T12:30:12Z
      _dbfconfig:
        refid: "cpp"
`),
		ModTime: time.Now(),
	},
	"base/posts.dbf.yaml": &fstest.MapFile{
		Data: []byte(`posts:
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
        posts_tags:
          rows:
            - post_id: !dbfexpr "parent:post_id"
              tag_id: !dbfexpr "refid:tags:go:tag_id"
      _dbfconfig:
        refid: "post_1"
        tags: ["initial"]
    - post_id: 2
      parent_post_id: !dbfexpr "refid:posts:post_1:post_id"
      title: "Post 2"
      text: "This is the text of the seco d post"
      user_id: !dbfexpr "refid:users:johndoe:user_id"
      created_at: !!timestamp 2023-01-02T12:30:12Z
      updated_at: !!timestamp 2023-01-02T12:30:12Z
      _dbfdeps:
        posts_tags:
          rows:
            - post_id: !dbfexpr "parent:post_id"
              tag_id: !dbfexpr "refid:tags:javascript:tag_id"
        comments:
          rows:
            - comment_id: 3
              post_id: !dbfexpr "parent:post_id"
              user_id: !dbfexpr "refid:users:janedoe:user_id"
              text: "I liked this post!"
`),
		ModTime: time.Now(),
	},
	"base/posts_tags.dbf.yaml": &fstest.MapFile{
		Data: []byte(`posts_tags:
  config:
    table_name: "public.post_tag"
`),
		ModTime: time.Now(),
	},
	"base/comments.dbf.yaml": &fstest.MapFile{
		Data: []byte(`comments:
  config:
    depends:
      - posts
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
`),
		ModTime: time.Now(),
	},
	"test1/posts_test1.dbf.yaml": &fstest.MapFile{
		Data: []byte(`posts:
  rows:
    - post_id: 5
      title: "Post 5"
      text: "This is the text of the fifth post"
      user_id: !dbfexpr "refid:users:janedoe:user_id"
      created_at: !!timestamp 2023-01-05T12:30:12Z
      updated_at: !!timestamp 2023-01-05T12:30:12Z
      _dbfdeps:
        posts_tags:
          rows:
            - post_id: !dbfexpr "parent:post_id"
              tag_id: !dbfexpr "refid:tags:javascript:tag_id"
`),
		ModTime: time.Now(),
	},
	"test1/inner/posts_test1.dbf.yaml": &fstest.MapFile{
		Data: []byte(`posts:
  rows:
    - post_id: 6
      title: "Post 6"
      text: "This is the text of the sixth post"
      user_id: !dbfexpr "refid:users:janedoe:user_id"
      created_at: !!timestamp 2023-01-05T12:30:12Z
      updated_at: !!timestamp 2023-01-05T12:30:12Z
      _dbfdeps:
        posts_tags:
          rows:
            - post_id: !dbfexpr "parent:post_id"
              tag_id: !dbfexpr "refid:tags:go:tag_id"
`),
		ModTime: time.Now(),
	},
	"base/ignored.yaml": &fstest.MapFile{
		Data: []byte(`ignored_table:
  config:
    table_name: "public.ignored"
`),
		ModTime: time.Now(),
	},
}
