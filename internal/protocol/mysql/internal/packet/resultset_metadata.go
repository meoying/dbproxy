package packet

type ResultSetMetadata byte

const (
	// RESULTSET_METADATA_NONE
	// No metadata will be sent.
	RESULTSET_METADATA_NONE ResultSetMetadata = 0

	// RESULTSET_METADATA_FULL
	// The server will send all metadata.
	RESULTSET_METADATA_FULL = 1
)
