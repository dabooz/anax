package agreementbot

import (
	"bytes"
	"fmt"
	"github.com/open-horizon/anax/cutil"
	"github.com/open-horizon/anax/externalpolicy"
	"time"
)

type ServicePolicyEntry struct {
	Policy  *externalpolicy.ExternalPolicy `json:"policy,omitempty"`      // The service policy from the exchange.
	Updated uint64                         `json:"updatedTime,omitempty"` // The time when this entry was updated.
	Hash    []byte                         `json:"hash,omitempty"`        // A hash of the service policy to compare for changes in the exchange.
}

func (p *ServicePolicyEntry) String() string {
	return fmt.Sprintf("ServicePolicyEntry: "+
		"Updated: %v "+
		"Hash: %x "+
		"Policy: %v",
		p.Updated, p.Hash, p.Policy)
}

func (p *ServicePolicyEntry) ShortString() string {
	return p.String()
}

func NewServicePolicyEntry(p *externalpolicy.ExternalPolicy, svcId string) (*ServicePolicyEntry, error) {
	pSE := new(ServicePolicyEntry)
	pSE.Updated = uint64(time.Now().Unix())
	if hash, err := cutil.HashObject(p); err != nil {
		return nil, err
	} else {
		pSE.Hash = hash
	}
	pSE.Policy = p

	return pSE, nil
}

func (s *ServicePolicyEntry) UpdateServicePolicyEntry(p *externalpolicy.ExternalPolicy) (bool, error) {

	s.Updated = uint64(time.Now().Unix())
	s.Policy = p
	if hash, err := cutil.HashObject(p); err != nil {
		return true, err
	} else if !bytes.Equal(hash, s.Hash) {
		s.Hash = hash
		return true, nil
	} else {
		return false, nil
	}
}
