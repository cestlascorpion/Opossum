package utils

const (
	ServerAddr = ":8080"
	SkipIPV6   = true
)

const (
	ErrInvalidParameter   = "invalid parameter"
	ErrInvalidNodeKey     = "invalid node key"
	ErrInvalidClockTime   = "invalid clock time"
	ErrInvalidWorkerId    = "invalid worker id"
	ErrNoWorkerId         = "no worker id available"
	ErrWorkerLeaseExpired = "worker lease expired"
	ErrWorkerIdInUse      = "worker id in use"
	ErrInvalidTagKey      = "invalid tag key"
)
