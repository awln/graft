package raft
import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	crand "crypto/rand"
	"encoding/base64"
	"sync/atomic"
)


func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const nraft = 3
	var rfa []*Raft = make([]*Raft, nraft)
	var rfh []string = make([]string, nraft)
	
}