package keeper

const (
	opCreate       = 1
	opDelete       = 2
	opExists       = 3
	opGetData      = 4
	opSetData      = 5
	opGetAcl       = 6
	opSetAcl       = 7
	opGetChildren  = 8
	opSync         = 9
	opPing         = 11
	opGetChildren2 = 12
	opCheck        = 13
	opMulti        = 14
	opCreate2      = 15
	opClose        = -11
	opSetAuth      = 100
	opSetWatches   = 101
)

const (
	// Everything is OK
	errOk = 0

	//
	// System and server-side errors.
	//

	//
	// Generic system error: This is never thrown by the server, it
	// shouldn't be used other than to indicate a range.
	errSystemError = -1
	// A runtime inconsistency was found
	errRuntimeInconsistency = -2
	// A data inconsistency was found
	errDataInconsistency = -3
	// Connection to the server has been lost
	errConnectionLoss = -4
	// Error while marshalling or unmarshalling data
	errMarshallingError = -5
	// Operation is unimplemented
	errUnimplemented = -6
	// Operation timeout
	errOperationTimeout = -7
	// Invalid arguments
	errBadArguments = -8
	// Unknown session (internal server use only)
	errUnknownSession = -12
	// No quorum of new config is connected and up-to-date with the leader
	// of last commmitted config - try invoking reconfiguration after new
	// servers are connected and synced
	errNewConfigNoQuorum = -13
	// Another reconfiguration is in progress -- concurrent reconfigs not
	// supported (yet)
	errReconfigInProgress = -14

	//
	// API errors.
	//

	// Generic API error: This is never thrown by the server, it shouldn't
	// be used other than to indicate a range.
	errAPIError = -100
	// Node does not exist
	errNoNode = -101
	// Not authenticated
	errNoAuth = -102
	// Version conflict
	// In case of reconfiguration: reconfig requested from config version X
	// but last seen config has a different version Y
	errBadVersion = -103
	// Ephemeral nodes may not have children
	errNoChildrenForEphemerals = -108
	// The node already exists
	errNodeExists = -110
	// The node has children
	errNotEmpty = -111
	// The session has been expired by the server
	errSessionExpired = -112
	// Invalid callback specified
	errInvalidCallback = -113
	// Invalid ACL specified
	errInvalidACL = -114
	// Client authentication failed
	errAuthFailed = -115
	// Session moved to another server, so operation is ignored
	errSessionMoved = -118
	// State-changing request is passed to read-only server
	errNotReadOnly = -119
	// Attempt to create ephemeral node on a local session
	errEphemeralOnLocalSession = -120
	// Attempts to remove a non-existing watcher
	errNoWatcher = -121
)

const (
	flagEphemeral = 1
	flagSequence  = 2
)
