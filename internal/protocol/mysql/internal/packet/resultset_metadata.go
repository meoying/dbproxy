package packet

// ResultSetMetadata Flag specifying if metadata are skipped or not.
// https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#aba06d1157f6dee3f20537154103c91a1
type ResultSetMetadata byte

const (
	// ResultSetMetadataNone
	// No metadata will be sent.
	ResultSetMetadataNone ResultSetMetadata = 0

	// ResultSetMetadataFull
	// The server will send all metadata.
	ResultSetMetadataFull = 1
)
