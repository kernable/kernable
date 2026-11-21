package kit

import kernable "github.com/kernable/kernable/sdks/go"

// internal is the context of the kit's own follow-up commands (the event's, followed): they pass
// the middleware as internal. Without a listener context, a bare internal one.
func internal(c *kernable.ListenerContext) *kernable.RequestContext {
	r := kernable.RequestContext{}
	if c != nil {
		r = c.Follow()
	}
	r.BypassAuthorization = true
	return &r
}

// isRejected is whether err is a rejection with one of the codes.
func isRejected(err error, codes ...string) bool {
	r, ok := err.(*kernable.Rejected)
	if !ok {
		return false
	}
	for _, c := range codes {
		if r.Code == c {
			return true
		}
	}
	return false
}
