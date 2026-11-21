package kit

import "embed"

// Source is the kit's own files, for `kernable-sdk add kit`: the copy it writes into your
// project. Not part of the copy.
//
//go:embed *.go LICENSE README.md example fixtures
var Source embed.FS
