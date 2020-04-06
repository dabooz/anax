package agreementbot

import (
	"bytes"
	"fmt"
	"github.com/open-horizon/anax/businesspolicy"
	"github.com/open-horizon/anax/cutil"
	"github.com/open-horizon/anax/externalpolicy"
	"github.com/open-horizon/anax/policy"
	"reflect"
	"time"
)

type BusinessPolicyEntry struct {
	Policy          *policy.Policy                 `json:"policy,omitempty"`          // the metadata for this business policy from the exchange, it is converted to the internal policy format
	Updated         uint64                         `json:"updatedTime,omitempty"`     // the time when this entry was updated
	Hash            []byte                         `json:"hash,omitempty"`            // a hash of the business policy to compare for metadata changes in the exchange
	ServicePolicies map[string]*ServicePolicyEntry `json:"servicePolicies,omitempty"` // map of the service id and service policies
}

// Create a new BusinessPolicyEntry. It converts the businesspolicy to internal policy format.
// The business policy exchange id (org/id) is the header name for the internal generated policy.
func NewBusinessPolicyEntry(pol *businesspolicy.BusinessPolicy, polId string) (*BusinessPolicyEntry, error) {
	pBE := new(BusinessPolicyEntry)
	pBE.Updated = uint64(time.Now().Unix())
	if hash, err := cutil.HashObject(pol); err != nil {
		return nil, err
	} else {
		pBE.Hash = hash
	}
	pBE.ServicePolicies = make(map[string]*ServicePolicyEntry, 0)

	// Validate and convert the exchange business policy to internal policy format.
	if err := pol.Validate(); err != nil {
		return nil, fmt.Errorf("Failed to validate the deployment policy %v. %v", *pol, err)
	} else if pPolicy, err := pol.GenPolicyFromBusinessPolicy(polId); err != nil {
		return nil, fmt.Errorf("Failed to convert the deployment policy to internal policy format: %v. %v", *pol, err)
	} else {
		pBE.Policy = pPolicy
	}

	return pBE, nil
}

func (p *BusinessPolicyEntry) String() string {
	return fmt.Sprintf("DeploymentPolicyEntry: "+
		"Updated: %v "+
		"Hash: %x "+
		"Policy: %v"+
		"ServicePolicies: %v",
		p.Updated, p.Hash, p.Policy, p.ServicePolicies)
}

func (p *BusinessPolicyEntry) ShortString() string {
	keys := make([]string, 0, len(p.ServicePolicies))
	for k, _ := range p.ServicePolicies {
		keys = append(keys, k)
	}
	return fmt.Sprintf("DeploymentPolicyEntry: "+
		"Updated: %v "+
		"Hash: %x "+
		"Policy: %v"+
		"ServicePolicies: %v",
		p.Updated, p.Hash, p.Policy.Header.Name, keys)
}

// Return true if this entry has a reference to the input service id. This function assumes
// that the caller holds the correct policy manager lock.
func (p *BusinessPolicyEntry) hasServicePolicy(id string) bool {
	_, ok := p.ServicePolicies[id]
	return ok
}

// Return the service policy entry for the inpyt service id. This function assumes
// that the caller holds the correct policy manager lock.
func (p *BusinessPolicyEntry) getServicePolicy(id string) *ServicePolicyEntry {
	if p.hasServicePolicy(id) {
		return p.ServicePolicies[id]
	}
	return nil
}

// Add a service policy to a BusinessPolicyEntry.
// Returns true if there is an existing entry for the service id, and it is updated with the new policy.
// If the old and new service policies are same, it returns false.
func (p *BusinessPolicyEntry) AddServicePolicy(svcPolicy *externalpolicy.ExternalPolicy, svcId string) (bool, error) {
	if svcPolicy == nil || svcId == "" {
		return false, nil
	}

	pSE, err := NewServicePolicyEntry(svcPolicy, svcId)
	if err != nil {
		return false, err
	}

	servicePol, found := p.ServicePolicies[svcId]
	if !found {
		p.ServicePolicies[svcId] = pSE
		return false, nil
	} else {
		if !bytes.Equal(pSE.Hash, servicePol.Hash) {
			p.ServicePolicies[svcId] = pSE
			p.Updated = uint64(time.Now().Unix())
			return true, nil
		} else {
			// same service policy exists, do nothing
			return false, nil
		}
	}
}

// Remove a service policy from a BusinessPolicyEntry.
// It returns true if the service policy exists and is removed.
func (p *BusinessPolicyEntry) RemoveServicePolicy(svcId string) bool {
	if svcId == "" {
		return false
	}

	spe, found := p.ServicePolicies[svcId]
	if !found {
		return false
	} else {
		// An empty policy is also tracked in the business policy manager, this way we know if there is
		// new service policy added later.
		// The business policy manager does not track all the service policies referenced by a business policy.
		// It only tracks the ones that have agreements associated with it.
		tempPol := new(externalpolicy.ExternalPolicy)
		if !reflect.DeepEqual(*tempPol, *spe.Policy) {
			delete(p.ServicePolicies, svcId)

			// update the timestamp
			p.Updated = uint64(time.Now().Unix())
			return true
		} else {
			return false
		}
	}
}

func (pe *BusinessPolicyEntry) DeleteAllServicePolicies(org string) {
	pe.ServicePolicies = make(map[string]*ServicePolicyEntry, 0)
}

func (p *BusinessPolicyEntry) UpdateEntry(pol *businesspolicy.BusinessPolicy, polId string, newHash []byte) (*policy.Policy, error) {
	p.Hash = newHash
	p.Updated = uint64(time.Now().Unix())
	p.ServicePolicies = make(map[string]*ServicePolicyEntry, 0)

	// validate and convert the exchange business policy to internal policy format
	if err := pol.Validate(); err != nil {
		return nil, fmt.Errorf("Failed to validate the deployment policy %v. %v", *pol, err)
	} else if pPolicy, err := pol.GenPolicyFromBusinessPolicy(polId); err != nil {
		return nil, fmt.Errorf("Failed to convert the deployment policy to internal policy format: %v. %v", *pol, err)
	} else {
		p.Policy = pPolicy
		return pPolicy, nil
	}
}
