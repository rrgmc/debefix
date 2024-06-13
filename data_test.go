package debefix

import (
	"testing/fstest"
	"time"
)

var testFS = fstest.MapFS{
	"base/users.dbf.yaml": &fstest.MapFile{
		Data: []byte(`tables:
  users:
    config:
      table_name: "public.user"
    rows:
      - user_id: 1
        _refid: !refid "johndoe"
        name: "John Doe"
        email: "john@example.com"
        created_at: !!timestamp 2023-01-01T12:30:12Z
        updated_at: !!timestamp 2023-01-01T12:30:12Z
      - user_id: 2
        _refid: !refid "janedoe"
        _tags: !tags ["onlyone"]
        name: "Jane Doe"
        email: "jane@example.com"
        created_at: !!timestamp 2023-01-04T12:30:12Z
        updated_at: !!timestamp 2023-01-04T12:30:12Z
`),
		ModTime: time.Now(),
	},
	"base/tags.dbf.yaml": &fstest.MapFile{
		Data: []byte(`tables:
  tags:
    config:
      table_name: "tag"
    rows:
      - tag_id: !expr "generated"
        _refid: !refid "go"
        name: "Go"
        created_at: !!timestamp 2023-01-01T12:30:12Z
        updated_at: !!timestamp 2023-01-01T12:30:12Z
      - tag_id: !expr "generated"
        _refid: !refid "javascript"
        name: "JavaScript"
        created_at: !!timestamp 2023-01-01T12:30:12Z
        updated_at: !!timestamp 2023-01-01T12:30:12Z
      - tag_id: !expr "generated"
        _refid: !refid "cpp"
        name: "C++"
        created_at: !!timestamp 2023-01-01T12:30:12Z
        updated_at: !!timestamp 2023-01-01T12:30:12Z
`),
		ModTime: time.Now(),
	},
	"base/posts.dbf.yaml": &fstest.MapFile{
		Data: []byte(`tables:
  posts:
    config:
      table_name: "public.post"
    rows:
      - post_id: 1
        _refid: !refid "post_1"
        _tags: !tags ["initial"]
        title: "Post 1"
        text: "This is the text of the first post"
        user_id: !expr "refid:users:johndoe:user_id"
        created_at: !!timestamp 2023-01-01T12:30:12Z
        updated_at: !!timestamp 2023-01-01T12:30:12Z
        deps:
          !deps
          posts_tags:
            rows:
              - post_id: !expr "parent:post_id"
                tag_id: !expr "refid:tags:go:tag_id"
      - post_id: 2
        parent_post_id: !expr "refid:posts:post_1:post_id"
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
                tag_id: !expr "refid:tags:javascript:tag_id"
          comments:
            rows:
              - comment_id: 3
                post_id: !expr "parent:post_id"
                user_id: !expr "refid:users:janedoe:user_id"
                text: "I liked this post!"
`),
		ModTime: time.Now(),
	},
	"base/posts_tags.dbf.yaml": &fstest.MapFile{
		Data: []byte(`tables:
  posts_tags:
    config:
      table_name: "public.post_tag"
`),
		ModTime: time.Now(),
	},
	"base/comments.dbf.yaml": &fstest.MapFile{
		Data: []byte(`tables:
  comments:
    config:
      depends:
        - posts
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
`),
		ModTime: time.Now(),
	},
	"test1/posts_test1.dbf.yaml": &fstest.MapFile{
		Data: []byte(`tables:
  posts:
    rows:
      - post_id: 5
        title: "Post 5"
        text: "This is the text of the fifth post"
        user_id: !expr "refid:users:janedoe:user_id"
        created_at: !!timestamp 2023-01-05T12:30:12Z
        updated_at: !!timestamp 2023-01-05T12:30:12Z
        deps:
          !deps
          posts_tags:
            rows:
              - post_id: !expr "parent:post_id"
                tag_id: !expr "refid:tags:javascript:tag_id"
`),
		ModTime: time.Now(),
	},
	"test1/inner/posts_test1.dbf.yaml": &fstest.MapFile{
		Data: []byte(`tables:
  posts:
    rows:
      - post_id: 6
        title: "Post 6"
        text: "This is the text of the sixth post"
        user_id: !expr "refid:users:janedoe:user_id"
        created_at: !!timestamp 2023-01-05T12:30:12Z
        updated_at: !!timestamp 2023-01-05T12:30:12Z
        deps:
          !deps
          posts_tags:
            rows:
              - post_id: !expr "parent:post_id"
                tag_id: !expr "refid:tags:go:tag_id"
`),
		ModTime: time.Now(),
	},
	"base/ignored.yaml": &fstest.MapFile{
		Data: []byte(`tables:
  ignored_table:
    config:
      table_name: "public.ignored"
`),
		ModTime: time.Now(),
	},
}
