package debefix

import "context"

// Process define processes that can run alongside a Resolve operation, like a kind of plugin.
type Process interface {
	Start(ctx context.Context) (context.Context, error)
	Finish(ctx context.Context) error
}
