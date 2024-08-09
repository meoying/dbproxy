package packet

// https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a1c6cf2629b0bda6da6788a25725d6b7f

// SessionState Type of state change information that the server can include in the Ok packet.
// Note:
// session_state_type shouldn't go past 255 (i.e. 1-byte boundary).
// Modify the definition of SESSION_TRACK_END when a new member is added.
type SessionState byte

const (
	// SESSION_TRACK_SYSTEM_VARIABLES
	// Session system variables.
	SESSION_TRACK_SYSTEM_VARIABLES SessionState = iota

	// SESSION_TRACK_SCHEMA
	// Current schema.
	SESSION_TRACK_SCHEMA

	// SESSION_TRACK_STATE_CHANGE
	// track session state changes
	SESSION_TRACK_STATE_CHANGE

	// SESSION_TRACK_GTIDS
	// See also: session_track_gtids.
	SESSION_TRACK_GTIDS

	// SESSION_TRACK_TRANSACTION_CHARACTERISTICS
	// Transaction chistics.
	SESSION_TRACK_TRANSACTION_CHARACTERISTICS

	// SESSION_TRACK_TRANSACTION_STATE
	// Transaction state.
	SESSION_TRACK_TRANSACTION_STATE
)
